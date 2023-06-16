import requests
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def get_available_services():
    url = "https://www.red.cl/restservice_v2/rest/getservicios/all"
    response = requests.get(url)
    data = response.json()
    return data

def get_service_info(service_code):
    url = f"https://www.red.cl/restservice_v2/rest/conocerecorrido?codsint={service_code}"
    response = requests.get(url)
    data = response.json()
    return data

class APICall(beam.DoFn):
    def _init_(self, base_url):
        self.base_url = base_url
        
    def process(self, element):
        url = f"{self.base_url}{element}"
        response = requests.get(url)
        data = response.json()
        yield data

def run():
    available_services = get_available_services()
    
    options = PipelineOptions()
    with beam.Pipeline(options=options) as pipeline:
        services = (
            pipeline
            | "Create Services" >> beam.Create(available_services)
        )
        
        service_info = (
            services
            | "Get Service Info" >> beam.ParDo(APICall(base_url="https://www.red.cl/restservice_v2/rest/conocerecorrido?codsint="))
        )
        
       
        service_info | beam.Map(print)
        
        pipeline.run()

if _name_ == '_main_':
    run()