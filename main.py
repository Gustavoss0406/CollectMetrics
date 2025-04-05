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
        # URL e parâmetros para buscar apenas campanhas ativas
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
            logging.error(f"Erro durante a requisição das campanhas: {e}")
            raise HTTPException(status_code=502, detail=f"Erro de conexão: {str(e)}")
        
        campaigns_list = campaigns_data.get("data", [])
        total_active_campaigns = len(campaigns_list)
        
        # Função auxiliar para buscar insights individuais para cada campanha
        async def get_campaign_insights(camp):
            campaign_id = camp.get("id", "")
            # Inicializa com valores padrão (mantendo a estrutura original)
            campaign_obj = {
                "id": campaign_id,
                "nome_da_campanha": camp.get("name", ""),
                "cpc": "0.00",
                "impressions": 0,
                "clicks": 0,
                "ctr": "0.00%"
            }
            campaign_insights_url = f"https://graph.facebook.com/v16.0/{campaign_id}/insights"
            # Inclui os campos spend e actions para agregação dos dados globais
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
                        camp_impressions = float(item.get("impressions", 0))
                    except:
                        camp_impressions = 0.0
                    try:
                        camp_clicks = float(item.get("clicks", 0))
                    except:
                        camp_clicks = 0.0
                    ctr_value = (camp_clicks / camp_impressions * 100) if camp_impressions > 0 else 0.0
                    campaign_obj["impressions"] = int(camp_impressions)
                    campaign_obj["clicks"] = int(camp_clicks)
                    campaign_obj["ctr"] = format_percentage(ctr_value)
                    try:
                        camp_cpc = float(item.get("cpc", 0))
                    except:
                        camp_cpc = 0.0
                    campaign_obj["cpc"] = format_currency(camp_cpc)
                    
                    # Extração dos valores de spend e ações para agregação
                    try:
                        camp_spend = float(item.get("spend", 0))
                    except:
                        camp_spend = 0.0
                    camp_conversions = 0.0
                    camp_engagement = 0.0
                    for action in item.get("actions", []):
                        try:
                            value = float(action.get("value", 0))
                        except:
                            value = 0.0
                        if action.get("action_type") == "offsite_conversion":
                            camp_conversions += value
                        if action.get("action_type") in ["page_engagement", "post_engagement", "post_reaction"]:
                            camp_engagement += value
                    # Armazena valores auxiliares para a agregação global
                    campaign_obj["_spend"] = camp_spend
                    campaign_obj["_conversions"] = camp_conversions
                    campaign_obj["_engagement"] = camp_engagement
            except Exception as e:
                logging.error(f"Erro ao buscar insights para campanha {campaign_id}: {e}")
            return campaign_obj
        
        # Obtenção da lista de campanhas ativas com insights individuais
        tasks = [get_campaign_insights(camp) for camp in campaigns_list]
        recent_campaignsMA = await asyncio.gather(*tasks)
        recent_campaigns_total = len(recent_campaignsMA)
        
        # Agregação dos dados globais a partir das campanhas ativas
        total_impressions = sum(camp.get("impressions", 0) for camp in recent_campaignsMA)
        total_clicks = sum(camp.get("clicks", 0) for camp in recent_campaignsMA)
        global_ctr_value = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0.0
        total_spent = sum(camp.get("_spend", 0) for camp in recent_campaignsMA)
        aggregated_cpc = (total_spent / total_clicks) if total_clicks > 0 else 0.0
        total_conversions = sum(camp.get("_conversions", 0) for camp in recent_campaignsMA)
        total_engagement = sum(camp.get("_engagement", 0) for camp in recent_campaignsMA)
        
        # Remove os campos auxiliares para manter a estrutura original nas campanhas
        for camp in recent_campaignsMA:
            camp.pop("_spend", None)
            camp.pop("_conversions", None)
            camp.pop("_engagement", None)
        
        result = {
            "active_campaigns": total_active_campaigns,
            "total_impressions": total_impressions,
            "total_clicks": total_clicks,
            "ctr": format_percentage(global_ctr_value),
            "cpc": format_currency(aggregated_cpc),
            "conversions": total_conversions,
            "spent": total_spent,
            "engajamento": total_engagement,
            "recent_campaigns_total": recent_campaigns_total,
            "recent_campaignsMA": recent_campaignsMA
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
