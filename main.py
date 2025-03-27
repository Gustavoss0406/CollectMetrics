import asyncio
import json
import aiohttp
from fastapi import FastAPI, HTTPException, Body

app = FastAPI()

async def fetch_metrics(account_id: str, access_token: str):
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
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise Exception(f"Erro {resp.status}: {text}")
                return await resp.json()
        
        # Executa as duas requisições em paralelo
        insights_data, campaigns_data = await asyncio.gather(
            fetch(insights_url, params_insights),
            fetch(campaigns_url, params_campaigns)
        )
        
        if "data" not in insights_data or not insights_data["data"]:
            raise Exception("Nenhum dado de insights encontrado.")
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
        return result

@app.post("/metrics")
async def get_metrics(payload: dict = Body(...)):
    """
    Endpoint único que recebe um JSON com "account_id" e "access_token" e retorna:
      - Active Campaigns
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
    try:
        result = await fetch_metrics(account_id, access_token)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return result
