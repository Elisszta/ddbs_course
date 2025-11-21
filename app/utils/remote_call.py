from typing import Any

import aiohttp

from app.utils.settings import settings


async def remote_call(url: str, method: str = 'GET', headers=None, params=None, json=None) -> tuple[int, Any] | tuple[None, None]:
    if headers is None:
        headers = {'Authorization': 'Bearer ' + settings.db_api_secret}
    if params is None:
        params = {}
    async with aiohttp.ClientSession() as session:
        if method == 'GET':
            func = session.get
        elif method == 'POST':
            func = session.post
        elif method == 'PUT':
            func = session.put
        elif method == 'DELETE':
            func = session.delete
        elif method == 'PATCH':
            func = session.patch
        else:
            raise NotImplementedError
        try:
            if json is not None:
                async with func(url, headers=headers, params=params, json=json) as resp:
                    return resp.status, await resp.json()
            else:
                async with func(url, headers=headers, params=params) as resp:
                    return resp.status, await resp.json()
        except Exception as e:
            print(e)
            return None, None
