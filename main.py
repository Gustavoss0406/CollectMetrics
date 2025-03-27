from fastapi import FastAPI, HTTPException
import os
import requests
import json

app = FastAPI()

# As credenciais são obtidas via variáveis de ambiente.
# Certifique-se de configurar as variáveis META_ADS_ACCOUNT_ID e META_ADS_ACCESS_TOKEN no ambiente do Railway.
ACCOUNT_ID = os.getenv("META_ADS_ACCOUNT_ID")   # Exemplo: "609309324904292"
ACCESS_TOKEN = os.getenv("META_ADS_ACCESS_TOKEN") # Exemplo: seu token de 60 dias

@app.get("/metrics")
def get_metrics():
    # Verifica se as credenciais estão configuradas
    if not ACCOUNT_ID or not ACCESS_TOKEN:
        raise HTTPException(status_code=500, detail="Credenciais não configuradas corretamente.")
    
    # Requisição para Insights (métricas)
    insights_url = f"https://graph.facebook.com/v16.0/act_{ACCOUNT_ID}/insights"
    params_insights = {
        "fields": "impressions,clicks,ctr,spend,cpc,actions,date_start,date_stop",
        "date_preset": "maximum",
        "access_token": ACCESS_TOKEN
    }
    
    response_insights = requests.get(insights_url, params=params_insights)
    if response_insights.status_code != 200:
        raise HTTPException(status_code=response_insights.status_code, detail=response_insights.text)
    
    insights_data = response_insights.json()
    
    # Requisição para obter as campanhas ativas
    filtering = json.dumps([{
        "field": "effective_status",
        "operator": "IN",
        "value": ["ACTIVE"]
    }])
    
    campaigns_url = f"https://graph.facebook.com/v16.0/act_{ACCOUNT_ID}/campaigns"
    params_campaigns = {
        "fields": "id,name,status",
        "filtering": filtering,
        "access_token": ACCESS_TOKEN
    }
    
    response_campaigns = requests.get(campaigns_url, params=params_campaigns)
    if response_campaigns.status_code != 200:
        raise HTTPException(status_code=response_campaigns.status_code, detail=response_campaigns.text)
    
    campaigns_data = response_campaigns.json()
    total_active_campaigns = len(campaigns_data.get("data", []))
    
    # Usaremos a CTR como métrica interessante (ao invés de ROI)
    interesting_metric = None
    if "data" in insights_data and len(insights_data["data"]) > 0:
        interesting_metric = insights_data["data"][0].get("ctr", None)
    
    return {
        "insights": insights_data,
        "active_campaigns": campaigns_data,
        "total_active_campaigns": total_active_campaigns,
        "ctr": interesting_metric
    }
