from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.endpoints import ulm, ost, loan, locality, taxon, search, lots, login, species_stats, person, admin, data_file_processor, batch_review, taxon_review, family_policy, taxon_merge, genus_audit
from app.db.elasticsearch import init_es, close_es
from app.core.config import settings
from app.middleware.request_logging import RequestLoggingMiddleware

app = FastAPI(
    title="Fisheries Database API",
    description="API for fisheries collection management with Elasticsearch search capabilities",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Modify in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(
    RequestLoggingMiddleware,
    log_dir="logs",
    max_file_size=50 * 1024 * 1024,  # 50MB
    backup_count=10,
    exclude_paths=["/docs", "/redoc", "/openapi.json", "/favicon.ico", "/metrics"],
    log_format="json",
    log_request_body=False  # 设为False避免影响PUT请求性能
)


# Include routers from different modules
app.include_router(login.router, prefix="/api/login", tags=["Login"])
app.include_router(ulm.router, prefix="/api/ulm", tags=["ULM"])
app.include_router(ost.router, prefix="/api/ost", tags=["OST"])
app.include_router(loan.router, prefix="/api/loan", tags=["Loan"])
app.include_router(locality.router, prefix="/api/locality", tags=["Locality"])
app.include_router(taxon.router, prefix="/api/taxon", tags=["Taxonomy"])
app.include_router(search.router, prefix="/api/search", tags=["Search"])
app.include_router(lots.router, prefix="/api/lots", tags=["Lots"])
app.include_router(species_stats.router, prefix="/api/stats", tags=["stats"])
app.include_router(person.router, prefix="/api/person", tags=["Person"])
app.include_router(admin.router, prefix="/api/admin", tags=["Admin"])
app.include_router(data_file_processor.router, prefix="/api/file", tags=["File Processor"])
app.include_router(batch_review.router, prefix="/api/batch", tags=["Batch Processor"])
app.include_router(taxon_review.router, prefix="/api/synonym-review", tags=["Synonym Review"])
app.include_router(family_policy.router, prefix="/api/family-policy", tags=["Family Policy"])
app.include_router(taxon_merge.router, prefix="/api/taxon-merge", tags=["Taxon Merge"])
app.include_router(genus_audit.router, prefix="/api/genus-audit", tags=["Genus Audit"])
#
@app.on_event("startup")
async def startup_db_client():
    # ES 未启用时直接跳过；启用但连不上时也不让整个 app 起不来（DB 搜索不依赖 ES）
    if not settings.ES_ENABLED:
        print("[startup] ES_ENABLED=false, skipping Elasticsearch init")
        return
    try:
        await init_es()
    except Exception as e:
        print(f"[startup] Elasticsearch init failed, continuing without ES: {e}")

@app.on_event("shutdown")
async def shutdown_db_client():
    try:
        await close_es()
    except Exception as e:
        print(f"[shutdown] Elasticsearch close failed: {e}")

@app.get("/")
async def root():
    return {"message": "Welcome to the FMMT (Fish Management and Monitoring Tool) API"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)