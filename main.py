from fastapi import FastAPI, HTTPException, Body
import requests
import json

app = FastAPI()

@app.post("/metrics")
def get_metrics(payload: dict = Body(...)):
    """
    Recebe um JSON contendo "account_id" e "access_token" e retorna as métricas:
      - Active Campaigns (total de campanhas ativas)
      - Total Impressions
      - Total Clicks
      - CTR
      - CPC
      - Conversions (soma das conversões "offsite_conversion")
      - Spent
      - Engajamento (soma de "page_engagement", "post_engagement" e "post_reaction")
    """
    account_id = payload.get("account_id")
    access_token = payload.get("access_token")
    
    if not account_id or not access_token:
        raise HTTPException(status_code=400, detail="É necessário fornecer 'account_id' e 'access_token' no body.")
    
    # --- Requisição para Insights ---
    insights_url = f"https://graph.facebook.com/v16.0/act_{account_id}/insights"
    params_insights = {
        "fields": "impressions,clicks,ctr,spend,cpc,actions",
        "date_preset": "maximum",
        "access_token": access_token
    }
    
    response_insights = requests.get(insights_url, params=params_insights)
    if response_insights.status_code != 200:
        raise HTTPException(status_code=response_insights.status_code, detail=response_insights.text)
    
    insights_data = response_insights.json()
    
    if "data" not in insights_data or len(insights_data["data"]) == 0:
        raise HTTPException(status_code=404, detail="Nenhum dado de insights encontrado.")
    
    insights_item = insights_data["data"][0]
    
    try:
        impressions = float(insights_item.get("impressions", 0))
    except:
        impressions = 0
    try:
        clicks = float(insights_item.get("clicks", 0))
    except:
        clicks = 0
    ctr = insights_item.get("ctr", None)
    try:
        cpc = float(insights_item.get("cpc", 0))
    except:
        cpc = 0
    try:
        spent = float(insights_item.get("spend", 0))
    except:
        spent = 0
    
    # Conversions: soma dos valores da ação "offsite_conversion"
    conversions = 0.0
    # Engajamento: soma dos valores de "page_engagement", "post_engagement" e "post_reaction"
    engajamento = 0.0
    for action in insights_item.get("actions", []):
        action_type = action.get("action_type")
        try:
            value = float(action.get("value", 0))
        except:
            value = 0.0
        if action_type == "offsite_conversion":
            conversions += value
        if action_type in ["page_engagement", "post_engagement", "post_reaction"]:
            engajamento += value

    # --- Requisição para Campanhas Ativas ---
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
    
    response_campaigns = requests.get(campaigns_url, params=params_campaigns)
    if response_campaigns.status_code != 200:
        raise HTTPException(status_code=response_campaigns.status_code, detail=response_campaigns.text)
    
    campaigns_data = response_campaigns.json()
    total_active_campaigns = len(campaigns_data.get("data", []))
    
    return {
        "active_campaigns": total_active_campaigns,
        "total_impressions": impressions,
        "total_clicks": clicks,
        "ctr": ctr,
        "cpc": cpc,
        "conversions": conversions,
        "spent": spent,
        "engajamento": engajamento
    }
