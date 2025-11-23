from typing import Any

import aiohttp

from app.settings import settings


async def remote_db_call(
        url: str,
        method: str = 'GET',
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        json: Any = None,
        timeout: aiohttp.ClientTimeout = aiohttp.ClientTimeout(total=10) # 默认10秒超时
) -> tuple[int, Any] | tuple[None, str]:
    default_headers = {'Authorization': f'Bearer {settings.db_api_secret}'}
    final_headers = {**default_headers, **(headers or {})}
    # 使用getattr简化方法调用
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.request(method, url, headers=final_headers, params=params, json=json) as resp:
                try:
                    return resp.status, await resp.json()
                except Exception:
                    return resp.status, None
    except Exception as e:
        print(f'Remote error: {e}')
        return None, str(e)
