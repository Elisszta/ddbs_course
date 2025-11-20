from fastapi import FastAPI
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

app = FastAPI()

# 连接到 global_master 库
DATABASE_URL = "mysql+aiomysql://root:root@localhost:3306/global_master"
engine = create_async_engine(DATABASE_URL, echo=True)

@app.get("/")
async def root():
    async with engine.connect() as conn:
        # 执行一个简单的查询
        result = await conn.execute(text("SELECT msg FROM connection_test LIMIT 1"))
        row = result.fetchone()
        return {"message": f"Database says: {row[0]}"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)