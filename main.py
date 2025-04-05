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
    """Formata o CTR individual com 2 casas decimais e adiciona '%' (mantém a formatação original para campanhas)."""
    return f"{value:.2f}%"

def format_currency(value: float) -> str:
    """Formata o CPC individual com 2 casas decimais (mantém a formatação original para campanhas)."""
    return f"{value:.2f}"

def format_cpc_truncate(value: float) -> str:
    """
    Trunca o CPC global para 1 dígito decimal sem arredondar.
    Se o resultado (como string) tiver mais de 3 caracteres, utiliza somente a parte inteira.
    """
    # Trunca sem arredondar
    truncated = int(value * 10) / 10
    s = f"{truncated:.1f}"  # ex: "0.2"
    if len(s) > 3:
        s = str(int(truncated))
        s = s[:3]
    return s

def format_ctr_truncate(value: float) -> str:
    """
    Trunca o CTR global (em porcentagem) para 1 dígito decimal sem arredondar.
    Se o resultado (incluindo o '%') tiver mais de 3 caracteres, utiliza somente a parte inteira seguida de '%'.
    """
    truncated = int(value * 10) / 10
    s = f"{truncated:.1f}%"
    if len(s) > 3:
        s = f"{int(truncated)}%"
        if len(s) > 3:
            s = s[:3]
    return s

async def fetch_metrics(account_id: str, access_token: str):
    start_time = time.perf_counter()
    logging.debug(f"Iniciando fetch_metrics para account_id: {account_id}")
    
    timeout = aiohttp.ClientTimeout(total=3)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Busca as campanhas ativas
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
            logging.debug(f"HTTP REQUEST: GET {url} com params: {params}")
            async with session.get(url, params=params) as resp:
                req_end = time.perf_counter()
                response_time = req_end - req_start
                logging.debug(f"HTTP RESPONSE: {url} completado em {response_time:.3f} segundos com status {resp.status}")
                if resp.status != 200:
                    response_text = await resp.text()
                    logging.error(f"HTTP ERROR: {url} retornou status {resp.status} com resposta: {response_text}")
                    raise Exception(f"Erro {resp.status}: {response_text}")
                response_json = await resp.json()
                logging.debug(f"HTTP RESPONSE JSON: {url} retornou: {response_json}")
                return response_json
        
        async def get_campaign_insights(camp):
            campaign_id = camp.get("id", "")
            campaign_obj = {
                "id": campaign_id,
                "nome_da_campanha": camp.get("name", ""),
                "cpc": "0.00",
                "impressions": 0,
                "clicks": 0,
                "ctr": "0.00%"  # Mantém a formatação original para cada campanha
            }
            campaign_insights_url = f"https://graph.facebook.com/v16.0/{campaign_id}/insights"
            params_campaign_insights = {
                "fields": "impressions,clicks,ctr,cpc,spend,actions",
                "date_preset": "maximum",
                "access_token": access_token
            }
            metrics = {
                "impressions": 0.0,
                "clicks": 0.0,
                "spend": 0.0,
                "conversions": 0.0,
                "engagement": 0.0
            }
            try:
                campaign_insights = await fetch(campaign_insights_url, params_campaign_insights)
                if "data" in campaign_insights and campaign_insights["data"]:
                    item = campaign_insights["data"][0]
                    try:
                        camp_impressions = float(item.get("impressions", 0))
                    except Exception as e:
                        logging.error(f"Erro convertendo impressions para campanha {campaign_id}: {e}")
                        camp_impressions = 0.0
                    try:
                        camp_clicks = float(item.get("clicks", 0))
                    except Exception as e:
                        logging.error(f"Erro convertendo clicks para campanha {campaign_id}: {e}")
                        camp_clicks = 0.0
                    metrics["impressions"] = camp_impressions
                    metrics["clicks"] = camp_clicks
                    ctr_value = (camp_clicks / camp_impressions * 100) if camp_impressions > 0 else 0.0
                    campaign_obj["impressions"] = int(camp_impressions)
                    campaign_obj["clicks"] = int(camp_clicks)
                    campaign_obj["ctr"] = format_percentage(ctr_value)
                    try:
                        camp_cpc = float(item.get("cpc", 0))
                    except Exception as e:
                        logging.error(f"Erro convertendo cpc para campanha {campaign_id}: {e}")
                        camp_cpc = 0.0
                    campaign_obj["cpc"] = format_currency(camp_cpc)
                    try:
                        camp_spend = float(item.get("spend", 0))
                    except Exception as e:
                        logging.error(f"Erro convertendo spend para campanha {campaign_id}: {e}")
                        camp_spend = 0.0
                    metrics["spend"] = camp_spend
                    conversions = 0.0
                    engagement = 0.0
                    for action in item.get("actions", []):
                        try:
                            value = float(action.get("value", 0))
                        except Exception as e:
                            logging.error(f"Erro convertendo value em actions para campanha {campaign_id}: {e}")
                            value = 0.0
                        if action.get("action_type") == "offsite_conversion":
                            conversions += value
                        if action.get("action_type") in ["page_engagement", "post_engagement", "post_reaction"]:
                            engagement += value
                    metrics["conversions"] = conversions
                    metrics["engagement"] = engagement
                    logging.debug(f"Campanha {campaign_id} - Métricas calculadas: {metrics}")
                else:
                    logging.debug(f"Sem dados de insights para a campanha {campaign_id}.")
            except Exception as e:
                logging.error(f"Erro ao buscar insights para campanha {campaign_id}: {e}")
            return campaign_obj, metrics
        
        try:
            campaigns_data = await fetch(campaigns_url, params_campaigns)
        except Exception as e:
            logging.error(f"Erro durante a requisição de campanhas: {e}")
            raise HTTPException(status_code=502, detail=f"Erro de conexão: {str(e)}")
        
        campaigns_list = campaigns_data.get("data", [])
        campaign_results = []
        if campaigns_list:
            tasks = [get_campaign_insights(camp) for camp in campaigns_list]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, Exception):
                    logging.error(f"Erro em get_campaign_insights: {res}")
                else:
                    campaign_results.append(res)
        else:
            campaign_results = []
        
        # Agregação das métricas globais
        total_impressions = sum(metrics["impressions"] for _, metrics in campaign_results)
        total_clicks = sum(metrics["clicks"] for _, metrics in campaign_results)
        total_spend = sum(metrics["spend"] for _, metrics in campaign_results)
        total_conversions = sum(metrics["conversions"] for _, metrics in campaign_results)
        total_engagement = sum(metrics["engagement"] for _, metrics in campaign_results)
        
        global_ctr_value = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0.0
        global_cpc_value = (total_spend / total_clicks) if total_clicks > 0 else 0.0
        
        # Log dos valores globais antes da formatação final
        logging.debug(f"Valores globais brutos: impressions={total_impressions}, clicks={total_clicks}, spend={total_spend}")
        logging.debug(f"Global CTR (valor): {global_ctr_value} | Global CPC (valor): {global_cpc_value}")
        
        total_active_campaigns = len(campaign_results)
        recent_campaigns_total = total_active_campaigns
        recent_campaignsMA = [campaign_obj for campaign_obj, _ in campaign_results]
        
        result = {
            "active_campaigns": total_active_campaigns,
            "total_impressions": total_impressions,
            "total_clicks": total_clicks,
            "ctr": format_ctr_truncate(global_ctr_value),  # ex: "1%" ou "1.0%" (máx. 3 caracteres)
            "cpc": format_cpc_truncate(global_cpc_value),    # ex: "0.2" (máx. 3 caracteres)
            "conversions": total_conversions,
            "spent": total_spend,
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
    # Utilize a porta esperada pela sua aplicação FlutterFlow; neste exemplo, 8000.
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
