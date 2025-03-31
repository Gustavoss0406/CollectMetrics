import asyncio
import json
import time
import aiohttp
import logging
from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware

# Configuração do logging para debug
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

app = FastAPI()

# Habilita CORS para todas as origens (ajuste conforme necessário)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def fetch_metrics(account_id: str, access_token: str):
    start_time = time.perf_counter()
    logging.debug(f"Iniciando fetch_metrics para account_id: {account_id}")
    
    # Configura um timeout total de 3 segundos para evitar esperas longas
    timeout = aiohttp.ClientTimeout(total=3)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # URL e parâmetros para Insights
        insights_url = f"https://graph.facebook.com/v16.0/act_{account_id}/insights"
        params_insights = {
            "fields": "impressions,clicks,ctr,spend,cpc,actions",
            "date_preset": "maximum",
            "access_token": access_token
        }
        
        # URL e parâmetros para Campanhas Ativas
        campaigns_url = f"https://graph.facebook.com/v16.0/act_{account_id}/campaigns"
        filtering = json.dumps([{
            "field": "effective_status",
            "operator": "IN",
            "value": ["ACTIVE"]
        }])
        params_campaigns = {
            "fields": "id,name,status",
            "filtering": filtering,
            "access_token": access_token
        }
        
        # Função auxiliar para realizar requisições GET e tratar erros
        async def fetch(url, params):
            req_start = time.perf_counter()
            logging.debug(f"Iniciando requisição GET para {url} com params: {params}")
            async with session.get(url, params=params) as resp:
                req_end = time.perf_counter()
                logging.debug(f"Requisição para {url} completada em {req_end - req_start:.3f} segundos com status {resp.status}")
                if resp.status != 200:
                    text = await resp.text()
                    raise Exception(f"Erro {resp.status}: {text}")
                return await resp.json()
        
        try:
            insights_data, campaigns_data = await asyncio.gather(
                fetch(insights_url, params_insights),
                fetch(campaigns_url, params_campaigns)
            )
        except Exception as e:
            logging.error(f"Erro durante as requisições: {e}")
            raise HTTPException(status_code=502, detail=f"Erro de conexão: {str(e)}")
        
        if "data" not in insights_data or not insights_data["data"]:
            raise HTTPException(status_code=404, detail="Nenhum dado de insights encontrado.")
        insights_item = insights_data["data"][0]
        
        try:
            impressions = float(insights_item.get("impressions", 0))
        except:
            impressions = 0.0
        try:
            clicks = float(insights_item.get("clicks", 0))
        except:
            clicks = 0.0
        ctr = insights_item.get("ctr", None)
        try:
            cpc = float(insights_item.get("cpc", 0))
        except:
            cpc = 0.0
        try:
            spent = float(insights_item.get("spend", 0))
        except:
            spent = 0.0
        
        # Soma de conversões e engajamento
        conversions = 0.0
        engagement = 0.0
        for action in insights_item.get("actions", []):
            try:
                value = float(action.get("value", 0))
            except:
                value = 0.0
            if action.get("action_type") == "offsite_conversion":
                conversions += value
            if action.get("action_type") in ["page_engagement", "post_engagement", "post_reaction"]:
                engagement += value
        
        total_active_campaigns = len(campaigns_data.get("data", []))
        
        result = {
            "active_campaigns": total_active_campaigns,
            "total_impressions": impressions,
            "total_clicks": clicks,
            "ctr": ctr,
            "cpc": cpc,
            "conversions": conversions,
            "spent": spent,
            "engajamento": engagement
        }
        end_time = time.perf_counter()
        logging.debug(f"fetch_metrics concluído em {end_time - start_time:.3f} segundos para account_id: {account_id}")
        return result

@app.post("/metrics")
async def get_metrics(payload: dict = Body(...)):
    # Logs detalhados para verificar o body recebido
    logging.debug("==== Início da requisição para /metrics ====")
    logging.debug(f"Tipo do payload recebido: {type(payload)}")
    try:
        payload_str = json.dumps(payload, indent=2, ensure_ascii=False)
    except Exception as e:
        payload_str = str(payload)
    logging.debug(f"Payload completo: {payload_str}")
    
    account_id = payload.get("account_id")
    access_token = payload.get("access_token")
    if not account_id or not access_token:
        logging.error("Payload inválido: 'account_id' ou 'access_token' ausentes.")
        raise HTTPException(status_code=400, detail="É necessário fornecer 'account_id' e 'access_token' no body.")
    try:
        result = await fetch_metrics(account_id, access_token)
    except Exception as e:
        logging.error(f"Erro no endpoint /metrics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
