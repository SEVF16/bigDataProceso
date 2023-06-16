import requests
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def obtener_servicios_disponibles():
    url = "https://www.red.cl/restservice_v2/rest/getservicios/all"
    response = requests.get(url)
    data = response.json()
    return data

def obtener_informacion_servicio(codigo_servicio):
    url = f"https://www.red.cl/restservice_v2/rest/conocerecorrido?codsint={codigo_servicio}"
    response = requests.get(url)
    data = response.json()
    return data

def get_next_event(service):
    try:
        codigo_servicio = service['codigo_servicio']
    except KeyError as e:
        print(f"Error: Clave 'codigo_servicio' no encontrada en el diccionario 'service'")
        print("Contenido del diccionario 'service':")
        print(service)
        return []
    
    # Resto de la lógica para obtener el siguiente evento a partir del código del servicio
    eventos = [
        {
            'codigo_servicio': codigo_servicio,
            'evento': 'Evento 1',
            'descripcion': 'Descripción del evento 1'
        },
        {
            'codigo_servicio': codigo_servicio,
            'evento': 'Evento 2',
            'descripcion': 'Descripción del evento 2'
        }
    ]
    
    return eventos

class APICall(beam.DoFn):
    def __init__(self, base_url):
        self.base_url = base_url
        
    def process(self, element):
        url = f"{self.base_url}{element}"
        response = requests.get(url)
        data = response.json()
        yield data

def run():
    servicios_disponibles = obtener_servicios_disponibles()
    
    opciones = PipelineOptions()
    with beam.Pipeline(options=opciones) as canalizacion:
        servicios = (
            canalizacion
            | "Crear servicios" >> beam.Create(servicios_disponibles)
        )
        
        service_info = (
            servicios
            | "Obtener información del servicio" >> beam.ParDo(APICall(base_url="https://www.red.cl/restservice_v2/rest/conocerecorrido?codsint="))
        )
        
        events = service_info | beam.FlatMap(lambda service: get_next_event(service))
        
        (events
         | 'events:tostring' >> beam.Map(lambda fields: json.dumps(fields))
         | 'events:out' >> beam.io.textio.WriteToText('all_events')
         )
        
        canalizacion.run()

if __name__ == '__main__':
    run()
