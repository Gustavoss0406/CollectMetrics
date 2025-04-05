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

def format_percentage(value: float) -> str:
    """Formata um valor float como percentual com duas casas decimais."""
    return f"{value:.2f}%"

def format_currency(value: float) -> str:
    """Formata um valor float para string com duas casas decimais."""
    return f"{value:.2f}"

async def fetch_metrics(account_id: str, access_token: str):
    start_time = time.perf_counter()
    logging.debug(f"Iniciando fetch_metrics para account_id: {account_id}")

    # Timeout total de 3 segundos para evitar esperas longas
    timeout = aiohttp.ClientTimeout(total=3)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # URL e parâmetros para buscar campanhas ativas
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

        # Função auxiliar para realizar requisições GET
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

        # Busca as campanhas ativas
        try:
            campaigns_data = await fetch(campaigns_url, params_campaigns)
        except Exception as e:
            logging.error(f"Erro durante requisição de campanhas: {e}")
            raise HTTPException(status_code=502, detail=f"Erro de conexão: {str(e)}")

        campaigns_list = campaigns_data.get("data", [])
        if not campaigns_list:
            raise HTTPException(status_code=404, detail="Nenhuma campanha ativa encontrada.")

        # Função auxiliar para buscar insights individuais para cada campanha
        async def get_campaign_insights(camp):
            campaign_id = camp.get("id", "")
            # Inicializa o objeto da campanha com valores padrão
            campaign_obj = {
                "id": campaign_id,
                "nome_da_campanha": camp.get("name", ""),
                "impressions": 0,
                "clicks": 0,
                "spent": 0.0,
                "conversions": 0.0,
                "engajamento": 0.0,
                "cpc": "0.00",
                "ctr": "0.00%"
            }
            campaign_insights_url = f"https://graph.facebook.com/v16.0/{campaign_id}/insights"
            params_campaign_insights = {
                "fields": "impressions,clicks,ctr,cpc,spend,actions",
                "date_preset": "maximum",
                "access_token": access_token
            }
            try:
                campaign_insights = await fetch(campaign_insights_url, params_campaign_insights)
                if "data" in campaign_insights and campaign_insights["data"]:
                    item = campaign_insights["data"][0]
                    try:
                        impressions = float(item.get("impressions", 0))
                    except:
                        impressions = 0.0
                    try:
                        clicks = float(item.get("clicks", 0))
                    except:
                        clicks = 0.0
                    try:
                        spend = float(item.get("spend", 0))
                    except:
                        spend = 0.0
                    # Calcula conversões e engajamento a partir das ações
                    conversions = 0.0
                    engajamento = 0.0
                    for action in item.get("actions", []):
                        try:
                            value = float(action.get("value", 0))
                        except:
                            value = 0.0
                        if action.get("action_type") == "offsite_conversion":
                            conversions += value
                        if action.get("action_type") in ["page_engagement", "post_engagement", "post_reaction"]:
                            engajamento += value
                    campaign_obj["impressions"] = int(impressions)
                    campaign_obj["clicks"] = int(clicks)
                    ctr_value = (clicks / impressions * 100) if impressions > 0 else 0.0
                    campaign_obj["ctr"] = format_percentage(ctr_value)
                    campaign_obj["cpc"] = format_currency(spend / clicks if clicks > 0 else 0.0)
                    campaign_obj["spent"] = spend
                    campaign_obj["conversions"] = conversions
                    campaign_obj["engajamento"] = engajamento
            except Exception as e:
                logging.error(f"Erro ao buscar insights para campanha {campaign_id}: {e}")
            return campaign_obj

        # Busca os insights individuais das campanhas ativas em paralelo
        tasks = [get_campaign_insights(camp) for camp in campaigns_list]
        active_campaigns_insights = await asyncio.gather(*tasks)

        # Agrega as métricas globais apenas das campanhas ativas
        total_impressions = sum(camp["impressions"] for camp in active_campaigns_insights)
        total_clicks = sum(camp["clicks"] for camp in active_campaigns_insights)
        total_spent = sum(camp["spent"] for camp in active_campaigns_insights)
        total_conversions = sum(camp["conversions"] for camp in active_campaigns_insights)
        total_engajamento = sum(camp["engajamento"] for camp in active_campaigns_insights)
        global_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0.0
        global_cpc = (total_spent / total_clicks) if total_clicks > 0 else 0.0

        # Monta o resultado final
        result = {
            "active_campaigns": len(campaigns_list),
            "total_impressions": total_impressions,
            "total_clicks": total_clicks,
            "ctr": format_percentage(global_ctr),
            "cpc": format_currency(global_cpc),
            "conversions": total_conversions,
            "spent": total_spent,
            "engajamento": total_engajamento,
            "recent_campaigns_total": len(active_campaigns_insights),
            "recent_campaignsMA": active_campaigns_insights
        }
        end_time = time.perf_counter()
        logging.debug(f"fetch_metrics concluído em {end_time - start_time:.3f} segundos para account_id: {account_id}")
        return result

@app.post("/metrics")
async def get_metrics(payload: dict = Body(...)):
    logging.debug("==== Início da requisição para /metrics ====")
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
