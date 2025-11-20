from typing import Self

from pydantic import model_validator
from pydantic_settings import BaseSettings

# 环境变量配置

# DB_MASTER_SLAVE_URL 本地主从库的url
# DB_SHARD_URL 本地分库的url

# A_WEB_URL A校区web应用的url
# B_WEB_URL B校区web应用的url
# C_WEB_URL C校区web应用的url
# 哪个留空就表示当前是哪个校区
# 校区对应课程编号：A:10 B:11 C:12

# JWT_SECRET 用于生成jwt的密钥（全局统一）
# DB_API_SECRET 访问远程分片数据库API的密钥（全局统一）

class Settings(BaseSettings):
    db_master_slave_url: str
    db_shard_url: str
    campus_a_web_url: str | None = None
    campus_b_web_url: str | None = None
    campus_c_web_url: str | None = None
    jwt_secret: str
    db_api_secret: str

    @model_validator(mode='after')
    def check_campus(self) -> Self:
        if (self.campus_a_web_url is None and self.campus_b_web_url is not None and self.campus_c_web_url is not None
                or self.campus_a_web_url is not None and self.campus_b_web_url is None and self.campus_c_web_url is not None
                or self.campus_a_web_url is not None and self.campus_b_web_url is not None and self.campus_c_web_url is None):
            return self
        raise ValueError('Invalid campus setting')

    def current_campus(self) -> str:
        if self.campus_a_web_url is None:
            return 'A'
        if self.campus_b_web_url is None:
            return 'B'
        return 'C'

    def current_min_cid(self) -> int:
        if self.campus_a_web_url is None:
            return 1000000
        if self.campus_b_web_url is None:
            return 1100000
        return 1200000


# 实例化Settings对象
settings = Settings()
