import os
import redis
import uvicorn
import datetime
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

class UserInput(BaseModel):
    company: str
    startYr: int
    endYr: int

app = FastAPI(title = "DAMG 7245")

@app.post("/api/v1/fetch-companies-data")
async def fetchCompaniesData(data: UserInput) -> dict:
    #user input validations 
    if len(data.company) < 3:
        response={"msg": "Company name should be of atleast 3 characters", "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        raise HTTPException(status_code=400, detail=response)
    
    if data.startYr > data.endYr:
        response={"msg": "Start year cannot be greater than end year", "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        raise HTTPException(status_code=400, detail=response)
    
    if data.startYr > datetime.now().year and data.endYr > datetime.now().year:
        response={"msg": "Start year, End year cannot be greater than current year", "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        raise HTTPException(status_code=400, detail=response)
    
    try:
        redis_host = os.getenv('REDIS_DB_HOST', 'localhost')
        redis_port = int(os.getenv('REDIS_DB_PORT', 30001))

        client = redis.Redis(host=redis_host, port=redis_port, username="", password="", decode_responses=True)

        date_range = ""
        for year in range(data.startYr, data.endYr+1):
            date_range += "^" + str(year) + "*|"

        date_range = date_range[:-1]
        
        query="@company:{} @date:({})".format(data.company, date_range) 

        result = client.execute_command("FT.SEARCH", "idx:companies", query)

        even_elements = result[2::2]

        response = [{elements[i]: elements[i + 1] for i in range(0, len(elements), 2)} for elements in even_elements]

        return JSONResponse(status_code=200, content=response)

    except ConnectionError as e:
        response={"msg": "Server is down", "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

        raise HTTPException(status_code=500, detail=response)
    
host = os.getenv("FASTAPI_HOST", "localhost")
port = os.getenv("FASTAPI_PORT", 8000)

uvicorn.run(app, host=host, port=int(port))
    

    
    