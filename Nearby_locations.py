import pandas
import geopy

filename = 'DANE_consulta.csv' #Webscraping-CatastroData.csv
df = pandas.read_csv(filename, low_memory=False) #encoding = 'latin_1'
df['coordenadas'] = df['latitud'].astype(str) + ', '+ df['longitud'].astype(str)

coordinates = pandas.DataFrame()
test =  pandas.DataFrame()
coordinates['coordenadas'] = df['coordenadas']
test['coordenadas'] = coordinates.sample(50)

from geopy.extra.rate_limiter import AsyncRateLimiter
from geopy.adapters import AioHTTPAdapter
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from tqdm import tqdm
import asyncio

def lugares_cercanos_process(lugares_list, dist_list):
    for lugar in lugares_list:
        if lugar != '':
            dist_list[lugar] = average_list(dist_list[lugar])

def average_list(list):
    return round(sum(list)/len(list),2)

nombres_dict = {
    'police': 'policia',
    'hospital': 'hospital',
    'mall': 'centro comercial',
    'restaurant': 'restaurante',
    'school': 'escuela',
    'university': 'universidad',
    'bank': 'banco',
    'supermarket': 'supermercado',
    'convenience store': 'tienda',
    'grocery store': 'tienda',
    'gym': 'gimnasio',
    'park': 'parque',
    'bus stop': 'parada de autobus',
    'ATM': 'cajero',
    'church': 'iglesia',
    'veterinarian': 'veterinaria', 
    'laundry': 'lavanderia'
}  # 17 places in total

tqdm.pandas()
geolocator = Nominatim(user_agent="nearby_search")
async def find_nearby_places(coordenadas):
    places_list = []
    dist = {}
    
    for lugar in nombres_dict.keys():
        query = f"{lugar} near {coordenadas}"
        places = geolocator.geocode(query, exactly_one=False)
        if places:
            for place in places:
                place_coords = (place.latitude, place.longitude)
                place_distance = geodesic((coordenadas), place_coords).kilometers
                if (place_distance <= 1):
                    if lugar not in places_list:
                        places_list.append(lugar)
                        dist.setdefault(lugar, []).append(round(place_distance, 2))
                    else:
                        dist.setdefault(lugar, []).append(round(place_distance, 2))
    lugares_cercanos_process(places_list, dist)
    return places_list, dist

async def main():
    async with Nominatim(
    user_agent="nearby_search",
    adapter_factory=AioHTTPAdapter,
    ) as geolocator:
        
        geocode = AsyncRateLimiter(geolocator.geocode, min_delay_seconds=1)
        tasks = [find_nearby_places(coords) for coords in test.coordenadas]
        await asyncio.gather(*tasks)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())