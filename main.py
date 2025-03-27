from fastapi import FastAPI, HTTPException, Body
import json
import httpx
from aiocache import cached
from aiocache.serializers import JsonSerializer

app = FastAPI()

# Cria um cliente assíncrono global com timeout reduzido e conexão persistente
client = httpx.AsyncClient(timeout=5.0)

# Fechamento do cliente ao desligar a aplicação
@app.on_event("shutdown")
async def shutdown_event():
    await client.aclose()

# Função auxiliar com cache para otimizar chamadas repetidas
@cached(ttl=30, serializer=JsonSerializer())
async def fetch_metrics(account_id: str, access_token: str):
    # URL e parâmetros para Insights
    insights_url = f"https://graph.facebook.com/v16.0/act_{account_id}/insights"
    params_insights = {
        "fields": "impressions,clicks,ctr,spend,cpc,actions",
        "date_preset": "maximum",
        "access_token": access_token
    }
    
    # URL e parâmetros para campanhas ativas
    filtering = json.dumps([{
        "field": "effective_status",
        "operator": "IN",
        "value": ["ACTIVE"]
    }])
    campaigns_url = f"https://graph.facebook.com/v16.0/act_{account_id}/campaigns"
    params_campaigns = {
        "fields": "id,name,status",
        "filtering": filtering,
        "access_token": access_token
    }
    
    # Executa as duas requisições em paralelo
    try:
        insights_resp, campaigns_resp = await httpx.gather(
            client.get(insights_url, params=params_insights),
            client.get(campaigns_url, params=params_campaigns)
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Erro na conexão: {str(e)}")
    
    if insights_resp.status_code != 200:
        raise HTTPException(status_code=insights_resp.status_code, detail=insights_resp.text)
    if campaigns_resp.status_code != 200:
        raise HTTPException(status_code=campaigns_resp.status_code, detail=campaigns_resp.text)
    
    insights_data = insights_resp.json()
    campaigns_data = campaigns_resp.json()
    
    if "data" not in insights_data or len(insights_data["data"]) == 0:
        raise HTTPException(status_code=404, detail="Nenhum dado de insights encontrado.")
    
    insights_item = insights_data["data"][0]
    
    # Extração dos dados de insights
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
    
    return {
        "active_campaigns": total_active_campaigns,
        "total_impressions": impressions,
        "total_clicks": clicks,
        "ctr": ctr,
        "cpc": cpc,
        "conversions": conversions,
        "spent": spent,
        "engajamento": engagement
    }

@app.post("/metrics")
async def get_metrics(payload: dict = Body(...)):
    """
    Endpoint único que recebe um JSON com "account_id" e "access_token" e retorna:
      - Active Campaigns (número de campanhas ativas)
      - Total Impressions
      - Total Clicks
      - CTR
      - CPC
      - Conversions
      - Spent
      - Engajamento
    """
    account_id = payload.get("account_id")
    access_token = payload.get("access_token")
    
    if not account_id or not access_token:
        raise HTTPException(status_code=400, detail="É necessário fornecer 'account_id' e 'access_token' no body.")
    
    return await fetch_metrics(account_id, access_token)
