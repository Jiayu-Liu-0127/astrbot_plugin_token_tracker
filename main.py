import time
import traceback
import asyncio
from typing import Dict, TypedDict, Optional
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register
from astrbot.api import logger, AstrBotConfig
from astrbot.core.provider.entities import LLMResponse

# 定义结构化数据类型
class SessionStats(TypedDict):
    prompt: int
    completion: int
    total: int
    count: int
    start_time: float

class SessionData(TypedDict):
    current: Optional[SessionStats]
    last_token_time: Optional[float]
    session_start: float
    last_active_time: float
    pending_auto: bool

@register("Token_Tracker", "Lystars", 
          "输入/token以查看对话段token统计信息，支持自动统计、自动重置和自动清理", 
          "1.2.0")
class TokenTracker(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        # 使用结构化类型
        self.stats: Dict[str, SessionData] = {}
        # 并发控制：每个会话的锁
        self._session_locks: Dict[str, asyncio.Lock] = {}
        
        # 安全解析配置，带容错处理
        try:
            self.auto_interval_hours = self._safe_get_config_float(config, "interval_hours", 24.0, 1.0, 720.0)
        except ValueError as e:
            logger.error(f"配置解析失败: {e}，使用默认值24.0小时")
            self.auto_interval_hours = 24.0
        
        try:
            session_ttl_hours = self._safe_get_config_float(config, "session_ttl_hours", 72.0, 1.0, 720.0)
            self.session_ttl = session_ttl_hours * 60 * 60  # 转换为秒
        except ValueError as e:
            logger.error(f"配置解析失败: {e}，使用默认值72.0小时")
            self.session_ttl = 72.0 * 60 * 60
        
        # 清理性能优化：设置清理间隔（秒）
        self.cleanup_interval = 300  # 5分钟清理一次
        self.last_cleanup_time = time.monotonic()
        
        logger.info(f"TokenTracker插件已加载，自动统计间隔: {self.auto_interval_hours}小时，会话过期时间: {self.session_ttl/3600:.1f}小时")
    
    def _safe_get_config_float(self, config: AstrBotConfig, key: str, default: float, min_val: float, max_val: float) -> float:
        """安全获取配置浮点数，带范围验证"""
        try:
            value = config.get(key, default)
            if value is None:
                return default
            
            # 转换为浮点数
            float_value = float(value)
            
            # 验证范围
            if not (min_val <= float_value <= max_val):
                raise ValueError(f"配置项 '{key}' 的值 {float_value} 超出范围 [{min_val}, {max_val}]")
            
            return float_value
        except (ValueError, TypeError) as e:
            raise ValueError(f"配置项 '{key}' 解析失败: {e}")
    
    def _session_id(self, event: AstrMessageEvent) -> str:
        """生成稳定的会话ID，异常时使用降级策略"""
        try:
            platform = getattr(event, 'platform_name', 'unknown')
            session_id = event.get_session_id()
            
            # 尝试获取用户ID和群ID，提供更稳定的降级策略
            user_id = getattr(event, 'user_id', None)
            group_id = getattr(event, 'group_id', None)
            
            if platform and session_id:
                return f"{platform}_{session_id}"
            elif platform and user_id and group_id:
                return f"{platform}_user{user_id}_group{group_id}"
            elif platform and user_id:
                return f"{platform}_user{user_id}"
            elif platform:
                return f"{platform}_unknown_session"
            else:
                # 最后降级：使用事件类型和用户ID的组合
                event_type = type(event).__name__
                return f"{event_type}_user{user_id or 'unknown'}"
        except Exception as e:
            # 极端情况下的降级策略，但避免使用id(event)
            logger.warning(f"生成会话ID失败: {e}，使用稳定降级策略")
            try:
                # 尝试获取一些稳定字段
                platform = getattr(event, 'platform_name', 'unknown')
                user_id = getattr(event, 'user_id', 'unknown')
                return f"{platform}_fallback_user{user_id}"
            except Exception:
                # 最后的手段，但比id(event)稳定
                return "global_fallback_session"
    
    def _get_session_lock(self, sid: str) -> asyncio.Lock:
        """获取或创建会话锁 - 带并发安全的原子操作"""
        # 注：由于dict.setdefault是原子的，这里是线程安全的
        return self._session_locks.setdefault(sid, asyncio.Lock())
    
    def _init_session_stats(self, sid: str) -> SessionStats:
        """初始化或重置会话的当前统计"""
        if sid not in self.stats:
            self.stats[sid] = SessionData(
                current=None,
                last_token_time=None,
                session_start=time.monotonic(),
                last_active_time=time.monotonic(),
                pending_auto=False
            )
        
        # 重置当前统计字段，保持其他字段不变
        current_stats: SessionStats = {
            "prompt": 0, 
            "completion": 0, 
            "total": 0, 
            "count": 0,
            "start_time": time.monotonic()
        }
        self.stats[sid]["current"] = current_stats
        return current_stats
    
    def _ensure_session_initialized(self, sid: str) -> None:
        """确保会话已初始化，但不重置统计"""
        if sid not in self.stats:
            self.stats[sid] = SessionData(
                current=None,
                last_token_time=None,
                session_start=time.monotonic(),
                last_active_time=time.monotonic(),
                pending_auto=False
            )
    
    def _get_current_stats(self, sid: str) -> Optional[SessionStats]:
        """获取当前统计，如果不存在则初始化或返回None"""
        if sid not in self.stats:
            return None
        current = self.stats[sid]["current"]
        if current is None:
            # 尝试初始化一次
            return self._init_session_stats(sid)
        return current
    
    def _check_auto_token(self, sid: str) -> bool:
        """检查是否需要自动统计 - 纯检查函数，无副作用"""
        interval_seconds = self.auto_interval_hours * 60 * 60
        now = time.monotonic()
        
        if sid not in self.stats:
            return False
        
        data = self.stats[sid]
        
        # 只检查是否需要自动统计，不删除会话
        last_token_time = data["last_token_time"]
        session_start = data["session_start"]
        
        if last_token_time is None:
            # 从未执行过自动统计，从会话创建时间开始计算
            if now - session_start >= interval_seconds:
                return True
        else:
            # 已执行过至少一次自动统计，从上次统计时间计算
            if now - last_token_time >= interval_seconds:
                return True
        
        return False
    
    async def _execute_auto_token(self, event: AstrMessageEvent, sid: str):
        """执行自动统计并发送消息 - 防止TOCTOU竞态条件"""
        now = time.monotonic()
        
        # 首次检查
        if sid not in self.stats:
            return
        
        stats_data = self.stats[sid]
        current_stats = stats_data["current"]
        if current_stats is None or current_stats["count"] == 0:
            # 没有统计记录时更新状态
            stats_data["pending_auto"] = False
            stats_data["last_token_time"] = now
            stats_data["last_active_time"] = now
            return
        
        # 保存当前统计数据的副本，避免后续调用中被修改
        stats_copy = dict(current_stats)
        last_token_time = stats_data["last_token_time"]
        session_start = stats_data["session_start"]
        
        if last_token_time is None:
            elapsed_hours = (now - session_start) / 3600
        else:
            elapsed_hours = (now - last_token_time) / 3600
        
        # 生成自动统计信息
        auto_msg = f"""⏰ 定时Token统计（已{elapsed_hours:.1f}小时未查看）：
• 请求次数：{stats_copy['count']}次
• 输入Token：{stats_copy['prompt']}个
• 输出Token：{stats_copy['completion']}个
• 总计Token：{stats_copy['total']}个

（统计已重置，下一轮定时统计将在{self.auto_interval_hours}小时后进行）"""
        
        try:
            # 先发送消息
            await event.send(event.plain_result(auto_msg))
            
            # 发送成功后再修改状态（减少持有数据的时间窗口）
            if sid in self.stats:
                self._init_session_stats(sid)
                self.stats[sid]["last_token_time"] = now
                self.stats[sid]["last_active_time"] = now
                self.stats[sid]["pending_auto"] = False
                
                logger.info(f"自动统计执行成功: {sid}, 消耗={stats_copy['total']}tokens, 间隔={elapsed_hours:.1f}小时")
            
        except Exception as send_error:
            # 发送失败，保留统计数据
            logger.error(f"自动统计发送失败（统计保留）: {sid}, 错误: {send_error}")
            if sid in self.stats:
                self.stats[sid]["last_active_time"] = now
    
    @filter.on_llm_response()
    async def on_llm_response(self, event: AstrMessageEvent, resp: LLMResponse):
        sid = self._session_id(event)
        lock = self._get_session_lock(sid)
        
        async with lock:
            try:
                now = time.monotonic()  # 统一时间戳
                
                # 确保会话已初始化
                self._ensure_session_initialized(sid)
                
                # 更新最后活跃时间（无论是否有usage，用户发送消息就算活跃）
                self.stats[sid]["last_active_time"] = now
                
                # 检查是否需要自动统计（在记录新token之前检查）
                should_auto = self._check_auto_token(sid)
                if should_auto:
                    # 标记为待处理，将在本次回复后执行
                    self.stats[sid]["pending_auto"] = True
                
                # 记录token使用（带空值保护）
                usage = resp.raw_completion.usage if resp.raw_completion else None
                if usage:
                    stats = self._get_current_stats(sid)
                    if stats is not None:
                        # 安全处理usage字段，避免None值
                        prompt_tokens = int(usage.prompt_tokens or 0)
                        completion_tokens = int(usage.completion_tokens or 0)
                        total_tokens = int(usage.total_tokens or 0)
                        
                        stats["prompt"] += prompt_tokens
                        stats["completion"] += completion_tokens
                        stats["total"] += total_tokens
                        stats["count"] += 1
                        
                        logger.debug(f"记录token: {sid}, 本次={total_tokens}, 累计={stats['total']}")
                    else:
                        logger.warning(f"无法获取会话统计数据: {sid}")
                else:
                    # 即使没有usage也记录日志
                    logger.debug(f"收到LLM响应但无usage信息: {sid}")
                
                # 性能优化：按间隔清理过期会话
                self._cleanup_expired_sessions()
                
                # 如果有待处理的自动统计，执行它
                need_auto_token = sid in self.stats and self.stats[sid]["pending_auto"]
                if need_auto_token:
                    try:
                        await self._execute_auto_token(event, sid)
                    except Exception as auto_error:
                        logger.error(f"自动统计执行失败: {sid}, 错误: {auto_error}")
                        # 自动统计失败不影响主流程
                
            except (ValueError, TypeError, AttributeError) as e:
                # 可恢复的结构异常
                logger.warning(f"LLM响应处理遇到可恢复异常: {sid}, 错误: {e}, 堆栈: {traceback.format_exc(limit=3)}")
            except Exception as e:
                # 其他不可预见的异常
                logger.error(f"处理LLM响应遇到未预期异常: {sid}, 错误: {e}, 堆栈摘要: {traceback.format_exc(limit=10)}")
    
    @filter.command("token")
    async def show_token(self, event: AstrMessageEvent):
        """显示当前对话段的token统计"""
        sid = self._session_id(event)
        lock = self._get_session_lock(sid)
        
        async with lock:
            try:
                # 性能优化：按间隔清理过期会话
                self._cleanup_expired_sessions()
                
                now = time.monotonic()
                
                # 确保会话已初始化
                self._ensure_session_initialized(sid)
                
                # 更新最后活跃时间（用户使用命令也算活跃）
                self.stats[sid]["last_active_time"] = now
                self.stats[sid]["last_token_time"] = now
                self.stats[sid]["pending_auto"] = False
                
                # 获取当前统计
                current_stats = self.stats[sid]["current"]
                
                if current_stats is not None and current_stats["count"] > 0:
                    msg = f"""📊 本段对话Token统计：
• 请求次数：{current_stats['count']}次
• 输入Token：{current_stats['prompt']}个
• 输出Token：{current_stats['completion']}个
• 总计Token：{current_stats['total']}个

（统计已重置，下一轮对话重新开始计数）"""
                    
                    # 重置当前统计
                    self._init_session_stats(sid)
                    
                    logger.info(f"显示并重置统计: {sid}, 本段消耗={current_stats['total']}tokens")
                else:
                    msg = "当前暂无Token消耗记录。继续对话以开始统计。"
                    logger.debug(f"查询空统计: {sid}")
                
                yield event.plain_result(msg)
                
            except Exception as e:
                logger.error(f"处理/token命令失败: {sid}, 错误: {e}, 堆栈摘要: {traceback.format_exc(limit=5)}")
                yield event.plain_result("统计查询失败，请稍后重试。")
    
    def _cleanup_expired_sessions(self) -> int:
        """清理过期会话 - 基于最后活跃时间，带性能优化"""
        now = time.monotonic()  # 统一时间戳
        
        # 性能优化：检查是否需要清理
        if now - self.last_cleanup_time < self.cleanup_interval:
            return 0  # 未到清理间隔
        
        self.last_cleanup_time = now
        expired_sids = []
        
        # 先收集所有过期的 session ID，避免遍历时修改字典
        for sid, data in list(self.stats.items()):
            # 使用最后活跃时间判断过期
            last_active = data["last_active_time"]
            if now - last_active > self.session_ttl:
                expired_sids.append(sid)
        
        # 清理过期会话及其关联的锁
        for sid in expired_sids:
            del self.stats[sid]
            # 同时清理对应的锁，避免内存泄漏
            if sid in self._session_locks:
                del self._session_locks[sid]
            logger.debug(f"清理过期会话: {sid}")
        
        return len(expired_sids)
