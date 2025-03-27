import asyncio
import json
from fastapi import FastAPI, HTTPException, Body
import aiohttp
from aiocache import cached
from aiocache.serializers import JsonSerializer

app = FastAPI()

@cached(ttl=30, serializer=JsonSerializer())
async def fetch_metrics(account_id: str, access_token: str):
    # Configura um timeout total de 3 segundos para evitar esperas longas
    timeout = aiohttp.ClientTimeout(total=3)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Configurar URL e parâmetros para Insights
        insights_url = f"https://graph.facebook.com/v16.0/act_{account_id}/insights"
        params_insights = {
            "fields": "impressions,clicks,ctr,spend,cpc,actions",
            "date_preset": "maximum",
            "access_token": access_token
        }
        # Configurar URL e parâmetros para Campanhas Ativas
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
        
        # Função auxiliar para realizar GET e tratar erros
        async def fetch(url, params):
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise HTTPException(status_code=resp.status, detail=text)
                return await resp.json()
        
        try:
            # Executa as requisições de Insights e Campanhas em paralelo
            insights_data, campaigns_data = await asyncio.gather(
                fetch(insights_url, params_insights),
                fetch(campaigns_url, params_campaigns)
            )
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Erro de conexão: {str(e)}")
        
        # Verifica se há dados de insights
        if "data" not in insights_data or len(insights_data["data"]) == 0:
            raise HTTPException(status_code=404, detail="Nenhum dado de insights encontrado.")
        insights_item = insights_data["data"][0]
        
        # Extração dos dados de insights (convertendo para float onde for necessário)
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
      - Conversions (soma das conversões do tipo "offsite_conversion")
      - Spent
      - Engajamento (soma das ações de engajamento)
    """
    account_id = payload.get("account_id")
    access_token = payload.get("access_token")
    if not account_id or not access_token:
        raise HTTPException(status_code=400, detail="É necessário fornecer 'account_id' e 'access_token' no body.")
    return await fetch_metrics(account_id, access_token)
