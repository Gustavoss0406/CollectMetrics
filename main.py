import asyncio
import json
from fastapi import FastAPI, HTTPException, Body
import aiohttp
from aiocache import cached
from aiocache.serializers import JsonSerializer

app = FastAPI()

# Definindo um ClientSession global com timeout reduzido para conexões
# (Este client será criado para cada chamada via a função fetch_metrics_aiohttp, pois a sessão é fechada após a execução.)
# Se desejar, você pode gerenciar uma sessão global, mas deve cuidar do fechamento adequado ao shutdown.

@cached(ttl=30, serializer=JsonSerializer())
async def fetch_metrics_aiohttp(account_id: str, access_token: str):
    timeout = aiohttp.ClientTimeout(total=3)  # Total timeout de 3 segundos (ajuste conforme necessário)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Configurar URLs e parâmetros
        insights_url = f"https://graph.facebook.com/v16.0/act_{account_id}/insights"
        params_insights = {
            "fields": "impressions,clicks,ctr,spend,cpc,actions",
            "date_preset": "maximum",
            "access_token": access_token
        }
        
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

        # Função auxiliar para realizar GET e processar erros
        async def fetch(url, params):
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    text = await response.text()
                    raise HTTPException(status_code=response.status, detail=text)
                return await response.json()

        try:
            # Executa as requisições em paralelo
            insights_task, campaigns_task = await asyncio.gather(
                fetch(insights_url, params_insights),
                fetch(campaigns_url, params_campaigns)
            )
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Erro de conexão: {str(e)}")

        insights_data = insights_task
        campaigns_data = campaigns_task

        if "data" not in insights_data or len(insights_data["data"]) == 0:
            raise HTTPException(status_code=404, detail="Nenhum dado de insights encontrado.")
        insights_item = insights_data["data"][0]

        # Extração dos dados de insights com conversão para float
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
    Endpoint único que recebe um JSON com "account_id" e "access_token" no corpo,
    realiza chamadas assíncronas otimizadas e retorna as seguintes métricas:
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
    
    return await fetch_metrics_aiohttp(account_id, access_token)
