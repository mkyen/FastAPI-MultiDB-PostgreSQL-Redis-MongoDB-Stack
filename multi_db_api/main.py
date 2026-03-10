from fastapi import FastAPI, HTTPException, Depends, Request
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
import redis
import motor.motor_asyncio
import hashlib
import json
import time
import os
from dotenv import load_dotenv
import asyncio


load_dotenv()

app = FastAPI(title="Multi-DB Demo API")

POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://localhost:5432/multidb_demo")
engine = create_engine(POSTGRES_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class UserModel(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    password_hash = Column(String)
    email = Column(String, nullable=True)

class ProductModel(Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    price = Column(String)

Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
r = redis.from_url(REDIS_URL, decode_responses=True)

MONGO_URL = os.getenv("MONGO_URL", "mongodb://127.0.0.1:27017")
mongo_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URL)
mongo_db = mongo_client["multidb_demo"]
logs_collection = mongo_db["request_logs"]

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    duration = round(time.time() - start, 4)
    await logs_collection.insert_one({
        "method": request.method,
        "path": request.url.path,
        "status_code": response.status_code,
        "duration_sec": duration,
        "timestamp": time.time()
    })
    return response

class UserCreate(BaseModel):
    username: str
    password: str

class ProductCreate(BaseModel):
    name: str
    price: str

@app.get("/")
def root():
    return {"message": "Multi-DB API is running"}

@app.post("/register")
def register(user: UserCreate, db: Session = Depends(get_db)):
    existing = db.query(UserModel).filter(UserModel.username == user.username).first()
    if existing:
        raise HTTPException(status_code=400, detail="Username already exists")
    hashed = hashlib.sha256(user.password.encode()).hexdigest()
    new_user = UserModel(username=user.username, password_hash=hashed)
    db.add(new_user)
    db.commit()
    return {"message": f"User '{user.username}' registered (PostgreSQL)"}

@app.post("/login")
def login(user: UserCreate, db: Session = Depends(get_db)):
    hashed = hashlib.sha256(user.password.encode()).hexdigest()
    db_user = db.query(UserModel).filter(
        UserModel.username == user.username,
        UserModel.password_hash == hashed
    ).first()
    if not db_user:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    r.setex(f"session:{user.username}", 3600, "active")
    return {"message": "Login OK - Session cached in Redis (TTL: 1h)"}

@app.get("/session/{username}")
def check_session(username: str):
    session = r.get(f"session:{username}")
    if session:
        ttl = r.ttl(f"session:{username}")
        return {"status": "active", "ttl_seconds": ttl, "source": "Redis"}
    return {"status": "no active session"}

@app.get("/products")
def get_products(db: Session = Depends(get_db)):
    cache_key = "products:all"
    cached = r.get(cache_key)
    if cached:
        return {"source": "Redis cache", "data": json.loads(cached)}
    products = db.query(ProductModel).all()
    data = [{"id": p.id, "name": p.name, "price": p.price} for p in products]
    r.setex(cache_key, 60, json.dumps(data))
    return {"source": "PostgreSQL", "data": data}

@app.post("/products")
def create_product(product: ProductCreate, db: Session = Depends(get_db)):
    new_product = ProductModel(name=product.name, price=product.price)
    db.add(new_product)
    db.commit()
    r.delete("products:all")
    return {"message": f"Product '{product.name}' added (PostgreSQL, cache cleared)"}

@app.get("/logs")
async def get_logs():
    logs = []
    async for log in logs_collection.find({}, {"_id": 0}).sort("timestamp", -1).limit(10):
        logs.append(log)
    return {"source": "MongoDB", "last_10_requests": logs}


#Health Endpoint Added

@app.get("/health")
async def health_check(db: Session = Depends(get_db)):
    status = {}

    # PostgreSQL
    try:
        db.execute(__import__("sqlalchemy").text("SELECT 1"))
        status["postgresql"] = "ok"
    except Exception as e:
        status["postgresql"] = f"error: {e}"

    # Redis
    try:
        r.ping()
        status["redis"] = "ok"
    except Exception as e:
        status["redis"] = f"error: {e}"

    # MongoDB
    try:
        await mongo_client.admin.command("ping")
        status["mongodb"] = "ok"
    except Exception as e:
        status["mongodb"] = f"error: {e}"

    return status
@app.on_event("startup")
async def create_indexes():
    await mongo_db.products.create_index([("name", "text"), ("description", "text")])

@app.get("/search")
async def search_products(q: str):
    results = []
    async for doc in mongo_db.products.find(
        {"$text": {"$search": q}},
        {"_id": 0}
    ):
        results.append(doc)
    return {"query": q, "results": results}

@app.post("/notify")
async def publish_notification(message: str):
    r.publish("notifications", message)
    return {"published": message}

@app.get("/listen")
async def listen_notifications():
    pubsub = r.pubsub()
    pubsub.subscribe("notifications")
    messages = []
    timeout = time.time() + 3  # 3 saniye dinle
    while time.time() < timeout:
        msg = pubsub.get_message()
        if msg and msg["type"] == "message":
            messages.append(msg["data"])
        await asyncio.sleep(0.1)
    pubsub.unsubscribe("notifications")
    return {"messages": messages}
