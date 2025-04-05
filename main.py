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
        # URL e parâmetros para os insights globais da conta (não serão usados para os valores finais)
        insights_url = f"https://graph.facebook.com/v16.0/act_{account_id}/insights"
        params_insights = {
            "fields": "impressions,clicks,ctr,spend,cpc,actions",
            "date_preset": "maximum",
            "access_token": access_token
        }
        
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
        
        # Busca os dados de insights globais e as campanhas de forma paralela
        try:
            insights_data, campaigns_data = await asyncio.gather(
                fetch(insights_url, params_insights),
                fetch(campaigns_url, params_campaigns)
            )
        except Exception as e:
            logging.error(f"Erro durante as requisições: {e}")
            raise HTTPException(status_code=502, detail=f"Erro de conexão: {str(e)}")
        
        # Mesmo que os insights globais sejam obtidos, iremos sobrescrever os valores abaixo com os dados das campanhas ativas.
        if "data" not in insights_data or not insights_data["data"]:
            raise HTTPException(status_code=404, detail="Nenhum dado de insights encontrado.")
        insights_item = insights_data["data"][0]
        
        # Valores originais (serão substituídos)
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
        
        # Função auxiliar para buscar insights individuais para cada campanha
        async def get_campaign_insights(camp):
            campaign_id = camp.get("id", "")
            # Inicializa com valores padrão (sem alterar a estrutura do objeto retornado)
            campaign_obj = {
                "id": campaign_id,
                "nome_da_campanha": camp.get("name", ""),
                "cpc": "0.00",
                "impressions": 0,
                "clicks": 0,
                "ctr": "0.00%"
            }
            campaign_insights_url = f"https://graph.facebook.com/v16.0/{campaign_id}/insights"
            # Adicionamos os campos spend e actions para agregarmos os valores globais
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
                    # Calcula e formata o CTR da campanha
                    ctr_value = (camp_clicks / camp_impressions * 100) if camp_impressions > 0 else 0.0
                    campaign_obj["impressions"] = int(camp_impressions)
                    campaign_obj["clicks"] = int(camp_clicks)
                    campaign_obj["ctr"] = format_percentage(ctr_value)
                    try:
                        camp_cpc = float(item.get("cpc", 0))
                    except:
                        camp_cpc = 0.0
                    campaign_obj["cpc"] = format_currency(camp_cpc)
                    
                    # Coleta dados adicionais para agregação global
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
                    # Armazena valores auxiliares (não fazem parte da resposta final da campanha)
                    campaign_obj["_spend"] = camp_spend
                    campaign_obj["_conversions"] = camp_conversions
                    campaign_obj["_engagement"] = camp_engagement
            except Exception as e:
                logging.error(f"Erro ao buscar insights para campanha {campaign_id}: {e}")
            return campaign_obj
        
        # Obtenção da lista de campanhas com insights individuais
        campaigns_list = campaigns_data.get("data", [])
        if campaigns_list:
            tasks = [get_campaign_insights(camp) for camp in campaigns_list]
            recent_campaignsMA = await asyncio.gather(*tasks)
        else:
            recent_campaignsMA = []
        recent_campaigns_total = len(recent_campaignsMA)
        
        # Agregação dos dados globais com base apenas nos insights das campanhas ativas
        aggregated_impressions = sum(camp.get("impressions", 0) for camp in recent_campaignsMA)
        aggregated_clicks = sum(camp.get("clicks", 0) for camp in recent_campaignsMA)
        global_ctr_value = (aggregated_clicks / aggregated_impressions * 100) if aggregated_impressions > 0 else 0.0
        global_spent = sum(camp.get("_spend", 0) for camp in recent_campaignsMA)
        aggregated_cpc = (global_spent / aggregated_clicks) if aggregated_clicks > 0 else 0.0
        global_conversions = sum(camp.get("_conversions", 0) for camp in recent_campaignsMA)
        global_engagement = sum(camp.get("_engagement", 0) for camp in recent_campaignsMA)
        
        # Sobrescreve os valores globais com os dados agregados das campanhas ativas
        impressions = aggregated_impressions
        clicks = aggregated_clicks
        ctr = format_percentage(global_ctr_value)
        cpc = format_currency(aggregated_cpc)
        spent = global_spent
        conversions = global_conversions
        engagement = global_engagement
        
        # Remove os campos auxiliares antes de retornar a resposta
        for camp in recent_campaignsMA:
            camp.pop("_spend", None)
            camp.pop("_conversions", None)
            camp.pop("_engagement", None)
        
        result = {
            "active_campaigns": total_active_campaigns,
            "total_impressions": impressions,
            "total_clicks": clicks,
            "ctr": ctr,
            "cpc": cpc,
            "conversions": conversions,
            "spent": spent,
            "engajamento": engagement,
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
