import json
import time
import traceback
import asyncio
from pathlib import Path
from typing import Dict, TypedDict, Optional
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register, StarTools
from astrbot.api import logger, AstrBotConfig
from astrbot.core.provider.entities import LLMResponse

# 常量定义
DEFAULT_AUTO_INTERVAL_HOURS = 24.0      # 默认自动报告间隔（小时）
DEFAULT_SESSION_TTL_HOURS = 72.0        # 默认会话过期时间（小时）
DEFAULT_PERSIST_INTERVAL_SECONDS = 300  # 默认持久化间隔（秒）
SECONDS_PER_HOUR = 3600                 # 每小时秒数
SECONDS_PER_MINUTE = 60                 # 每分钟秒数
VERSION = "1.2.0"                  # 插件版本@register("Token_Tracker", "Lystars",

BUFFER_SIZE = 8192               # 文件缓冲区大小（8KB）

    
# 定义结构化数据类型

class SessionData(TypedDict):

    prompt: int
    completion: int
    total: int
    count: int
    session_start: float
    last_auto_time: float
    last_active_time: float
    "输入/token以查看token统计信息，支持自动统计，自动管理和数据持久化", 
    "1.2.0"
    "1.2.0"
class TokenTracker(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.stats: Dict[str, SessionData] = {}
        self._session_locks: Dict[str, asyncio.Lock] = {}

        self.auto_interval_hours = DEFAULT_AUTO_INTERVAL_HOURS

        try:
            self.auto_interval_hours = self._safe_get_config_float(config, "interval_hours", DEFAULT_AUTO_INTERVAL_HOURS, 1.0, 720.0)
        except ValueError as e:
            logger.error(f"配置解析失败: {e}，使用默认值DEFAULT_AUTO_INTERVAL_HOURS小时")

        self.session_ttl_hours = DEFAULT_SESSION_TTL_HOURS
        try:
            self.session_ttl_hours = self._safe_get_config_float(config, "session_ttl_hours", DEFAULT_SESSION_TTL_HOURS, 1.0, 720.0)
        except ValueError as e:
            logger.error(f"配置解析失败: {e}，使用默认值DEFAULT_SESSION_TTL_HOURS小时")
        self.session_ttl = self.session_ttl_hours * SECONDS_PER_MINUTE * SECONDS_PER_MINUTE  # 转换为秒

        # 时间基准管理：内存使用 monotonic，用于运行时间隔判断；持久化转为 wall-clock
        self._base_wall_time = time.time()
        self._base_mono = time.monotonic()

        # 持久化配置
        self.persist_enabled = True
        self.persist_interval = DEFAULT_PERSIST_INTERVAL_SECONDS  # 默认5分钟持久化一次
        self.last_persist_time = time.monotonic()

        try:
            persist_enabled_raw = config.get("persist_enabled", True)
            if isinstance(persist_enabled_raw, str):
                lower_val = persist_enabled_raw.strip().lower()
                self.persist_enabled = lower_val in ("1", "true", "yes", "on")
            else:
                self.persist_enabled = bool(persist_enabled_raw)
        except Exception:
            self.persist_enabled = True

        try:
            persist_minutes = self._safe_get_config_float(config, "persist_interval_minutes", 5.0, 1.0, SECONDS_PER_MINUTE)
            self.persist_interval = int(persist_minutes * SECONDS_PER_MINUTE)
        except ValueError as e:
            logger.error(f"持久化间隔配置解析失败: {e}，使用默认5分钟")
            self.persist_interval = DEFAULT_PERSIST_INTERVAL_SECONDS

        # 从 StarTools 读取标准数据目录，支持持久化还原
        try:
            data_dir = Path(StarTools.get_data_dir())
            data_dir.mkdir(parents=True, exist_ok=True)
            self._stats_file = data_dir / 'token_tracker_stats.json'
        except Exception as e:
            logger.error(f"无法获取数据目录，持久化功能禁用: {e}")
            self._stats_file = None

        self._load_stats()

        logger.info(f"TokenTracker插件已加载，自动统计间隔: {self.auto_interval_hours}小时，会话过期时间: {self.session_ttl_hours:.1f}小时，持久化已{ '启用' if self.persist_enabled else '禁用' }")
    
    def _safe_get_config_float(self, config: AstrBotConfig, key: str, default: float, min_val: float, max_val: float) -> float:
        """安全获取配置浮点数，带范围验证"""
        value = config.get(key, default)
        if value is None:
            return default

        try:
            float_value = float(value)
        except (ValueError, TypeError) as e:
            raise ValueError(f"配置项 '{key}' 解析失败: {e}")

        if not (min_val <= float_value <= max_val):
            raise ValueError(f"配置项 '{key}' 的值 {float_value} 超出范围 [{min_val}, {max_val}]")

        return float_value

    def _wall_to_mono(self, wall_ts: float) -> float:
        return self._base_mono + (wall_ts - self._base_wall_time)

    def _mono_to_wall(self, mono_ts: float) -> float:
        return self._base_wall_time + (mono_ts - self._base_mono)

    def _load_stats(self) -> None:
        """从持久化文件读取先前的会话统计，含细粒度容错"""
        if not self.persist_enabled or not self._stats_file:
            return

        try:
            if not self._stats_file.exists():
                return

            with open(self._stats_file, 'r', encoding='utf-8', buffering=BUFFER_SIZE) as f:
                raw = json.load(f)

            if not isinstance(raw, dict):
                return

            if self.stats:
                return

            loaded_count = 0
            for sid, data in raw.items():
                try:
                    if not isinstance(data, dict):
                        continue

                    prompt = int(data.get('prompt', 0) or 0)
                    completion = int(data.get('completion', 0) or 0)
                    total = int(data.get('total', 0) or 0)
                    count = int(data.get('count', 0) or 0)

                    session_start = data.get('session_start')
                    if session_start is None:
                        session_start = time.monotonic()
                    else:
                        try:
                            session_start = self._wall_to_mono(float(session_start))
                        except (TypeError, ValueError):
                            session_start = time.monotonic()

                    last_auto_time = data.get('last_auto_time')
                    if last_auto_time is None:
                        last_auto_time = session_start
                    else:
                        try:
                            last_auto_time = self._wall_to_mono(float(last_auto_time))
                        except (TypeError, ValueError):
                            last_auto_time = session_start

                    last_active_time = data.get('last_active_time')
                    if last_active_time is None:
                        last_active_time = session_start
                    else:
                        try:
                            last_active_time = self._wall_to_mono(float(last_active_time))
                        except (TypeError, ValueError):
                            last_active_time = session_start

                    self.stats[sid] = SessionData(
                        prompt=prompt,
                        completion=completion,
                        total=total,
                        count=count,
                        session_start=session_start,
                        last_auto_time=last_auto_time,
                        last_active_time=last_active_time
                    )
                    loaded_count += 1
                except Exception as e:
                    logger.debug(f"加载会话 {sid} 失败，已跳过: {e}")
                    continue

            logger.info(f"TokenTracker已从持久化数据恢复{loaded_count}个会话")

        except Exception as e:
            logger.warning(f"加载持久化会话数据失败: {e}")

    def _save_stats(self) -> None:
        """将当前会话统计持久化到本地文件（仅在启用持久化时执行）"""
        if not self.persist_enabled or not self._stats_file:
            return

        try:
            serialized = {}
            for sid, data in self.stats.items():
                serialized[sid] = {
                    'prompt': data['prompt'],
                    'completion': data['completion'],
                    'total': data['total'],
                    'count': data['count'],
                    'session_start': self._mono_to_wall(data['session_start']),
                    'last_auto_time': self._mono_to_wall(data['last_auto_time']),
                    'last_active_time': self._mono_to_wall(data['last_active_time'])
                }

            tmp_path = self._stats_file.with_suffix('.json.tmp')
            with open(tmp_path, 'w', encoding='utf-8', buffering=BUFFER_SIZE) as f:
                json.dump(serialized, f, ensure_ascii=False, indent=2)
            tmp_path.replace(self._stats_file)
        except Exception as e:
            logger.warning(f"持久化会话数据失败: {e}")

    def _maybe_persist_data(self) -> None:
        if not self.persist_enabled:
            return

        now = time.monotonic()
        if now - self.last_persist_time >= self.persist_interval:
            self._save_stats()
            self.last_persist_time = now

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
        """获取或创建会话锁 - 使用原子操作确保并发安全"""
        # 使用字典的setdefault方法，这是原子操作，避免竞态条件
        return self._session_locks.setdefault(sid, asyncio.Lock())
    def _create_default_session_data(self) -> SessionData:
        """创建并返回默认会话数据对象"""
        now = time.monotonic()
        return SessionData(
            prompt=0,
            completion=0,
            total=0,
            count=0,
            session_start=now,
            last_auto_time=now,
            last_active_time=now
        )

    def _ensure_session_initialized(self, sid: str) -> None:
        """确保会话已初始化"""
        if sid not in self.stats:
            self.stats[sid] = self._create_default_session_data()

    def _check_auto_token(self, sid: str) -> bool:
        """检查是否需要自动统计"""
        if sid not in self.stats:
            return False

        interval_seconds = self.auto_interval_hours * SECONDS_PER_MINUTE * SECONDS_PER_MINUTE
        now = time.monotonic()
        data = self.stats[sid]
        return (now - data['last_auto_time']) >= interval_seconds

    async def _execute_auto_token(self, event: AstrMessageEvent, sid: str):
        """执行自动统计并发送消息"""
        lock = self._get_session_lock(sid)
        async with lock:
            if sid not in self.stats:
                return
            stats_data = self.stats[sid]
            now = time.monotonic()
            elapsed_hours = (now - stats_data['last_auto_time']) / SECONDS_PER_HOUR

            msg = f"""⏰ 定时Token统计（已{elapsed_hours:.1f}小时未查看）：\n"""
            msg += f"• 请求次数：{stats_data['count']}次\n"
            msg += f"• 输入Token：{stats_data['prompt']}个\n"
            msg += f"• 输出Token：{stats_data['completion']}个\n"
            msg += f"• 总计Token：{stats_data['total']}个\n\n"
            msg += f"（已清理会话数据，下次使用将重新开始统计）"


        try:
            await event.send(event.plain_result(msg))
            logger.info(f"自动统计执行成功: {sid}, 总消耗={stats_data['total']}tokens, 间隔={elapsed_hours:.1f}小时")

            # 发送成功后，清理会话数据
            lock = self._get_session_lock(sid)
            async with lock:
                if sid in self.stats:
                    self._remove_session(sid)
                    self._maybe_persist_data()
        except Exception as send_error:
            logger.error(f"自动统计发送失败: {sid}, 错误: {send_error}")

    @filter.on_llm_response()
    async def on_llm_response(self, event: AstrMessageEvent, resp: LLMResponse):
        sid = self._session_id(event)
        lock = self._get_session_lock(sid)

        async with lock:
            try:
                now = time.monotonic()
                self._ensure_session_initialized(sid)

                # 更新最后活跃时间
                self.stats[sid]['last_active_time'] = now

                usage = resp.raw_completion.usage if resp.raw_completion else None
                if usage:
                    prompt_tokens = int(usage.prompt_tokens or 0)
                    completion_tokens = int(usage.completion_tokens or 0)
                    total_tokens = int(usage.total_tokens or 0)

                    session_stats = self.stats[sid]
                    session_stats['prompt'] += prompt_tokens
                    session_stats['completion'] += completion_tokens
                    session_stats['total'] += total_tokens
                    session_stats['count'] += 1

                    logger.debug(f"记录token: {sid}, 本次={total_tokens}, 累计={session_stats['total']}")
                else:
                    logger.debug(f"收到LLM响应但无usage信息: {sid}")

                need_auto_token = self._check_auto_token(sid)

                # 清理过期会话
                self._cleanup_expired_sessions()
            except (ValueError, TypeError, AttributeError) as e:
                logger.warning(f"LLM响应处理遇到可恢复异常: {sid}, 错误: {e}, 堆栈: {traceback.format_exc(limit=3)}")
                need_auto_token = False
            except Exception as e:
                logger.error(f"处理LLM响应遇到未预期异常: {sid}, 错误: {e}, 堆栈摘要: {traceback.format_exc(limit=10)}")
                need_auto_token = False

        # 锁释放后再发送自动统计消息（避免长时间持有锁）
        if need_auto_token:
            try:
                await self._execute_auto_token(event, sid)
            except Exception as auto_error:
                logger.error(f"自动统计执行失败: {sid}, 错误: {auto_error}")
                # 自动统计失败不影响主流程
        
        # 最后持久化（在锁外完成）
        self._maybe_persist_data()
    def _remove_session(self, sid: str) -> None:
        """删除指定会话的数据"""
        if sid in self.stats:
            del self.stats[sid]
        
        if sid in self._session_locks:
            del self._session_locks[sid]
        
        logger.debug(f"清理会话: {sid}")
        self._maybe_persist_data()

    def _cleanup_expired_sessions(self) -> int:
        """清理过期会话 - 基于最后活跃时间"""
        now = time.monotonic()
        expired_sids = []

        for sid, data in list(self.stats.items()):
            last_active = data["last_active_time"]
            if now - last_active > self.session_ttl:
                expired_sids.append(sid)

        for sid in expired_sids:
            self._remove_session(sid)
            logger.debug(f"清理过期会话: {sid}")

        if expired_sids:
            self._maybe_persist_data()

        return len(expired_sids)
    
    @filter.command("token")
    async def show_token(self, event: AstrMessageEvent):
        """显示当前会话总token统计，清空总消耗、重置定时器并清理会话数据"""
        sid = self._session_id(event)
        lock = self._get_session_lock(sid)

        async with lock:
            try:
                now = time.monotonic()
                self._ensure_session_initialized(sid)

                stats_data = self.stats[sid]
                msg = f"""📊 会话总Token统计：
• 请求次数：{stats_data['count']}次
• 输入Token：{stats_data['prompt']}个
• 输出Token：{stats_data['completion']}个
• 总计Token：{stats_data['total']}个

（已清空总消耗、重置定时器并清理会话数据）"""

                # 清空总消耗、重置定时器并清理会话数据
                self._remove_session(sid)

                self._maybe_persist_data()
                yield event.plain_result(msg)

            except Exception as e:
                logger.error(f"处理/token命令失败: {sid}, 错误: {e}, 堆栈摘要: {traceback.format_exc(limit=5)}")
                yield event.plain_result("统计查询失败，请稍后重试。")

    def on_unload(self) -> None:
        """插件卸载/停止时写盘当前会话统计，避免数据丢失（仅在启用持久化时）"""
        if not self.persist_enabled:
            return
        
        try:
            self._save_stats()
            logger.info("TokenTracker退出时已持久化会话统计数据")
        except Exception as e:
            logger.warning(f"TokenTracker退出持久化失败: {e}")

    async def on_unload_async(self):
        """兼容异步卸载回调"""
        self.on_unload()

