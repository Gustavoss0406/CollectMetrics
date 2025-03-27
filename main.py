import os
from fastapi import FastAPI, HTTPException
import requests
import json

app = FastAPI()

# Obtenha os valores do ambiente. 
# Certifique-se de configurar essas variáveis no ambiente onde a API será executada ou passar via configuração no FlutterFlow.
ACCOUNT_ID = os.getenv("META_ADS_ACCOUNT_ID")  # Exemplo: "609309324904292"
ACCESS_TOKEN = os.getenv("META_ADS_ACCESS_TOKEN")  # Exemplo: "EAAQlH3ZBPVJgBO50prjDk7LyEw2HjslhpUiPV0sSZALrpKYTR43KUYiPbLOIXu7qoZCIiNTkg0grcDbsJKg1l48SzkX1sZAspqZBDUfUZB8aRDFTzHGykIwgpltuVhoJoaXjzm0wduHTnKwViIGe86ZBwqZBp71ftUjq4sgnbjcDLk6ctkDIoqJfxIa1TdhRq32z"

@app.get("/")
def root():
    return {"message": "Bem-vindo à API Meta Ads com FastAPI"}

@app.get("/insights")
def get_insights():
    """
    Busca os insights (métricas) da conta e calcula o ROI.
    """
    if not ACCOUNT_ID or not ACCESS_TOKEN:
        raise HTTPException(status_code=500, detail="Credenciais não configuradas corretamente.")

    insights_url = f"https://graph.facebook.com/v16.0/act_{ACCOUNT_ID}/insights"
    params_insights = {
        "fields": "impressions,clicks,ctr,spend,cpc,actions,date_start,date_stop",
        "date_preset": "maximum",
        "access_token": ACCESS_TOKEN
    }
    
    response_insights = requests.get(insights_url, params=params_insights)
    if response_insights.status_code != 200:
        raise HTTPException(status_code=response_insights.status_code, detail=response_insights.text)
    
    data_insights = response_insights.json()

    # Calcula o ROI com base em uma ação "offsite_conversion".
    spend = 0.0
    revenue = 0.0
    if "data" in data_insights and len(data_insights["data"]) > 0:
        insights_item = data_insights["data"][0]
        spend = float(insights_item.get("spend", 0))
        
        # Procura uma ação do tipo "offsite_conversion" para representar a receita
        for action in insights_item.get("actions", []):
            if action.get("action_type") == "offsite_conversion":
                try:
                    revenue = float(action.get("value", 0))
                except Exception:
                    revenue = 0.0
                break
    
    roi = (revenue - spend) / spend if spend > 0 else 0.0

    return {
        "insights": data_insights,
        "roi": roi
    }

@app.get("/active_campaigns")
def get_active_campaigns():
    """
    Busca as campanhas ativas da conta.
    """
    if not ACCOUNT_ID or not ACCESS_TOKEN:
        raise HTTPException(status_code=500, detail="Credenciais não configuradas corretamente.")
    
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
    
    data_campaigns = response_campaigns.json()
    active_campaign_count = len(data_campaigns.get("data", []))
    
    return {
        "active_campaigns": data_campaigns,
        "total_active_campaigns": active_campaign_count
    }
