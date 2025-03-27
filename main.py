from fastapi import FastAPI, HTTPException, Body
import json
import httpx

app = FastAPI()

@app.post("/metrics")
async def get_metrics(payload: dict = Body(...)):
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
    
    # Configura os parâmetros para insights e campanhas
    insights_url = f"https://graph.facebook.com/v16.0/act_{account_id}/insights"
    params_insights = {
        "fields": "impressions,clicks,ctr,spend,cpc,actions",
        "date_preset": "maximum",
        "access_token": access_token
    }
    
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
    
    # Usando httpx.AsyncClient para fazer chamadas em paralelo
    async with httpx.AsyncClient(timeout=10.0) as client:
        insights_future = client.get(insights_url, params=params_insights)
        campaigns_future = client.get(campaigns_url, params=params_campaigns)
        
        insights_response, campaigns_response = await httpx.gather(insights_future, campaigns_future)
    
    if insights_response.status_code != 200:
        raise HTTPException(status_code=insights_response.status_code, detail=insights_response.text)
    if campaigns_response.status_code != 200:
        raise HTTPException(status_code=campaigns_response.status_code, detail=campaigns_response.text)
    
    insights_data = insights_response.json()
    campaigns_data = campaigns_response.json()
    
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
