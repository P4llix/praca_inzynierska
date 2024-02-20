import json
import requests, time
import datetime
import random

class cepik_operator:
    def __init__(self):
        self.base_url = "https://api.cepik.gov.pl/pojazdy?"
        self.parms = {
            'wojewodztwo': '',
            'data_od' : '',
            'data_do': '',
            'wszystkie_pola': 'pokaz-wszystkie-pola=true',
            'limit': 'limit=500',
            'sort': 'sort=marka,model,wersja'
        }
        self.rows = 0

    def set_dates(self, date_start, date_end):
        self.parms['data_od'] = str(date_start)
        self.parms['data_do'] = str(date_end)

    def set_wojewodztwo(self, wojewodztwo):
        self.parms['wojewodztwo'] = str(self.parms['wojewodztwo'])[0 : self.parms['wojewodztwo'].find('=')+1] + str(wojewodztwo)

    def make_api_request(self, url):
        retries = 20
        attemps = 0
        headers = { 'Content-Type': 'application/json; charset=utf-8' }

        while attemps < retries:
            attemps = attemps + 1
            try:
                response = requests.get(url, headers=headers, timeout=20)
                response.raise_for_status()
                print(url)
                return response.json()
            except requests.exceptions.RequestException as e:
                print(str(attemps) + ". Blad podczas pobierania z API:", e)
                continue
        return None
        
    def get_count(self):
        url = self.base_url + '&'.join(
            [
                'wojewodztwo=' + str(self.parms['wojewodztwo']), 
                'data-od=' + str(self.parms['data_od']), 
                'data-do=' + str(self.parms['data_do']), 
                'limit=1'
            ]
        )
        response = self.make_api_request(url)
        if response:
            self.rows = response['meta']['count']
        else:
            self.rows = 0

    def get_page_number(self, total):
        pages = [per_page for per_page in range(1, total//500+2)]
        return pages

    def parse_result(self, entity):
        self.attributes = {
            'cepik_id' : None,
            'marka' : None,
            'model' : None,
            'kategoria_pojazdu' : None,
            'typ' : None,
            'wariant' : None,
            'wersja' : None,
            'rodzaj_pojazdu' : None,
            'podrodzaj_pojazdu' : None,
            'przeznaczenie_pojazdu' : None,
            'pochodzenie' : None,
            'tabliczka_znamienowa' : None,
            'sposob_produckji' : None,
            'rok_produkcji' : None,
            'rejestracja_w_kraju_pierwsza' : None,
            'rejestracja_w_kraju_ostatnia' : None,
            'rejestracja_za_granica' : None,
            'pojemnosc_skokowa' : None,
            'moc_netto_silnika' : None,
            'moc_netto_silnika_hybrydowego' : None,
            'masa_wlasna' : None,
            'max_masa_calkowita' : None,
            'dopuszczalna_masa' : None,
            'dopuszczalna_ladownosc' : None,
            'max_ladownosc' : None,
            'liczba_osi' : None,
            'miejsca_ogolem' : None,
            'miejsca_siedzace' : None,
            'miejsca_stojace' : None,
            'rodzaj_paliwa' : None,
            'rodzaj_paliwa_alternatywnego_pierwszego' : None,
            'rodzaj_paliwa_alternatywnego_drugiego' : None,
            'srednie_zuzycie' : None,
            'poziom_emisji_co2' : None,
            'rodzaj_zawieszenia' : None,
            'wersja_wyposazenia' : None,
            'hak' : None,
            'katalizator' : None,
            'kierownica_po_prawej_stronie' : None,
            'kierownica_po_prawej_stronie_pierwotnie' : None,
            'kod_instytutu_transaportu_samochodowego' : None,
            'nazwa_producenta' : None,
            'pierwsza_rejestracja' : None,
            'wyrejestrowanie' : None,
            'przyczyna_wyrejestrowania_pojazdu' : None,
            'rejestracja_wojewodztwo' : None,
            'rejestracja_gmina' : None,
            'rejestracja_powiat' : None,
            'wlasciciel_wojewodztwo' : None,
            'wlasciciel_powiat' : None,
            'wlasciciel_gmina' : None,
            'wlasciciel_wojewodztwo_kod' : None,
            'poziom_emisji_co2_paliwo_alternatywne' : None,
            'wojewodztwo_kod' : None
        }

        # print(entity)

        if 'id' in entity:
            self.attributes['cepik_id'] = entity['id']

        if 'marka' in entity['attributes']:
            self.attributes['marka'] = entity['attributes']['marka']

        if 'model' in entity['attributes']:
            self.attributes['model'] = entity['attributes']['model']

        if 'kategoria-pojazdu' in entity['attributes']:
            self.attributes['kategoria_pojazdu'] = entity['attributes']['kategoria-pojazdu']

        if 'typ' in entity['attributes']:
            self.attributes['typ'] = entity['attributes']['typ']

        if 'wariant' in entity['attributes']:
            self.attributes['wariant'] = entity['attributes']['wariant']

        if 'wersja' in entity['attributes']:
            self.attributes['wersja'] = entity['attributes']['wersja']

        if 'rodzaj-pojazdu' in entity['attributes']:
            self.attributes['rodzaj_pojazdu'] = entity['attributes']['rodzaj-pojazdu']

        if 'podrodzaj-pojazdu' in entity['attributes']:
            self.attributes['podrodzaj_pojazdu'] = entity['attributes']['podrodzaj-pojazdu']

        if 'przeznaczenie-pojazdu' in entity['attributes']:
            self.attributes['przeznaczenie_pojazdu'] = entity['attributes']['przeznaczenie-pojazdu']

        if 'pochodzenie' in entity['attributes']:
            self.attributes['pochodzenie'] = entity['attributes']['pochodzenie']

        if 'rodzaj-tabliczki-znamionowej' in entity['attributes']:
            self.attributes['tabliczka_znamienowa'] = entity['attributes']['rodzaj-tabliczki-znamionowej']

        if 'sposob-produkcji' in entity['attributes']:
            self.attributes['sposob_produckji'] = entity['attributes']['sposob-produkcji']

        if 'rok-produkcji' in entity['attributes']:
            self.attributes['rok_produkcji'] = entity['attributes']['rok-produkcji']

        if 'data-pierwszej-rejestracji-w-kraju' in entity['attributes']:
            self.attributes['rejestracja_w_kraju_pierwsza'] = entity['attributes']['data-pierwszej-rejestracji-w-kraju']

        if 'data-ostatniej-rejestracji-w-kraju' in entity['attributes']:
            self.attributes['rejestracja_w_kraju_ostatnia'] = entity['attributes']['data-ostatniej-rejestracji-w-kraju']

        if 'data-rejestracji-za-granica' in entity['attributes']:
            self.attributes['rejestracja_za_granica'] = entity['attributes']['data-rejestracji-za-granica']

        if 'pojemnosc-skokowa-silnika' in entity['attributes']:
            self.attributes['pojemnosc_skokowa'] = entity['attributes']['pojemnosc-skokowa-silnika']

        if 'moc-netto-silnika' in entity['attributes']:
            self.attributes['moc_netto_silnika'] = entity['attributes']['moc-netto-silnika']

        if 'moc-netto-silnika-hybrydowego' in entity['attributes']:
            self.attributes['moc_netto_silnika_hybrydowego'] = entity['attributes']['moc-netto-silnika-hybrydowego']

        if 'masa-wlasna' in entity['attributes']:
            self.attributes['masa_wlasna'] = entity['attributes']['masa-wlasna']

        if 'max-masa-calkowita' in entity['attributes']:
            self.attributes['max_masa_calkowita'] = entity['attributes']['max-masa-calkowita']

        if 'dopuszczalna-masa-calkowita-zespolu-pojazdow' in entity['attributes']:
            self.attributes['dopuszczalna_masa'] = entity['attributes']['dopuszczalna-masa-calkowita-zespolu-pojazdow']

        if 'dopuszczalna-ladownosc' in entity['attributes']:
            self.attributes['dopuszczalna_ladownosc'] = entity['attributes']['dopuszczalna-ladownosc']

        if 'max-ladownosc' in entity['attributes']:
            self.attributes['max_ladownosc'] = entity['attributes']['max-ladownosc']

        if 'liczba-osi' in entity['attributes']:
            self.attributes['liczba_osi'] = entity['attributes']['liczba-osi']

        if 'liczba-miejsc-ogolem' in entity['attributes']:
            self.attributes['miejsca_ogolem'] = entity['attributes']['liczba-miejsc-ogolem']

        if 'liczba-miejsc-siedzacych' in entity['attributes']:
            self.attributes['miejsca_siedzace'] = entity['attributes']['liczba-miejsc-siedzacych']

        if 'liczba-miejsc-stojacych' in entity['attributes']:
            self.attributes['miejsca_stojace'] = entity['attributes']['liczba-miejsc-stojacych']

        if 'rodzaj-paliwa' in entity['attributes']:
            self.attributes['rodzaj_paliwa'] = entity['attributes']['rodzaj-paliwa']

        if 'rodzaj-pierwszego-paliwa-alternatywnego' in entity['attributes']:
            self.attributes['rodzaj_paliwa_alternatywnego_pierwszego'] = entity['attributes']['rodzaj-pierwszego-paliwa-alternatywnego']

        if 'rodzaj-drugiego-paliwa-alternatywnego' in entity['attributes']:
            self.attributes['rodzaj_paliwa_alternatywnego_drugiego'] = entity['attributes']['rodzaj-drugiego-paliwa-alternatywnego']

        if 'srednie-zuzycie-paliwa' in entity['attributes']:
            self.attributes['srednie_zuzycie'] = entity['attributes']['srednie-zuzycie-paliwa']

        if 'poziom-emisji-co2' in entity['attributes']:
            self.attributes['poziom_emisji_co2'] = entity['attributes']['poziom-emisji-co2']

        if 'rodzaj-zawieszenia' in entity['attributes']:
            self.attributes['rodzaj_zawieszenia'] = entity['attributes']['rodzaj-zawieszenia']

        if 'wyposazenie-i-rodzaj-urzadzenia-radarowego' in entity['attributes']:
            self.attributes['wersja_wyposazenia'] = entity['attributes']['wyposazenie-i-rodzaj-urzadzenia-radarowego']

        if 'hak' in entity['attributes'] and entity['attributes']['hak'] == True:
            self.attributes['hak'] = 1
        else:
            self.attributes['hak'] = 0

        if 'katalizator' in entity['attributes'] and entity['attributes']['katalizator'] == True:
            self.attributes['katalizator'] = 1
        else:
            self.attributes['katalizator'] = 0

        if 'kierownica-po-prawej-stronie' in entity['attributes'] and entity['attributes']['kierownica-po-prawej-stronie'] == True:
            self.attributes['kierownica_po_prawej_stronie'] = 1
        else:
            self.attributes['kierownica_po_prawej_stronie'] = 0

        if 'kierownica-po-prawej-stronie-pierwotnie' in entity['attributes'] and entity['attributes']['kierownica-po-prawej-stronie-pierwotnie'] == True:
            self.attributes['kierownica_po_prawej_stronie_pierwotnie'] = 1
        else:
            self.attributes['kierownica_po_prawej_stronie_pierwotnie'] = 0

        if 'kod-instytutu-transaportu-samochodowego' in entity['attributes']:
            self.attributes['kod_instytutu_transaportu_samochodowego'] = entity['attributes']['kod-instytutu-transaportu-samochodowego']

        if 'nazwa-producenta' in entity['attributes']:
            self.attributes['nazwa_producenta'] = entity['attributes']['nazwa-producenta']

        if 'data-pierwszej-rejestracji' in entity['attributes']:
            self.attributes['pierwsza_rejestracja'] = entity['attributes']['data-pierwszej-rejestracji']

        if 'data-wyrejestrowania-pojazdu' in entity['attributes']:
            self.attributes['wyrejestrowanie'] = entity['attributes']['data-wyrejestrowania-pojazdu']

        if 'przyczyna-wyrejestrowania-pojazdu' in entity['attributes']:
            self.attributes['przyczyna_wyrejestrowania_pojazdu'] = entity['attributes']['przyczyna-wyrejestrowania-pojazdu']

        if 'rejestracja-wojewodztwo' in entity['attributes']:
            self.attributes['rejestracja_wojewodztwo'] = entity['attributes']['rejestracja-wojewodztwo']

        if 'rejestracja-gmina' in entity['attributes']:
            self.attributes['rejestracja_gmina'] = entity['attributes']['rejestracja-gmina']

        if 'rejestracja-powiat' in entity['attributes']:
            self.attributes['rejestracja_powiat'] = entity['attributes']['rejestracja-powiat']

        if 'wlasciciel-wojewodztwo' in entity['attributes']:
            self.attributes['wlasciciel_wojewodztwo'] = entity['attributes']['wlasciciel-wojewodztwo']

        if 'wlasciciel-powiat' in entity['attributes']:
            self.attributes['wlasciciel_powiat'] = entity['attributes']['wlasciciel-powiat']

        if 'wlasciciel-gmina' in entity['attributes']:
            self.attributes['wlasciciel_gmina'] = entity['attributes']['wlasciciel-gmina']

        if 'wlasciciel-wojewodztwo-kod' in entity['attributes']:
            self.attributes['wlasciciel_wojewodztwo_kod'] = entity['attributes']['wlasciciel-wojewodztwo-kod']

        if 'poziom-emisji-co2-paliwo-alternatywne-1' in entity['attributes']:
            self.attributes['poziom_emisji_co2_paliwo_alternatywne'] = entity['attributes']['poziom-emisji-co2-paliwo-alternatywne-1']

        if 'wojewodztwo-kod' in entity['attributes']:
            self.attributes['wojewodztwo_kod'] = entity['attributes']['wojewodztwo-kod']

    def get_entity_array(self):
        self.get_count()
        entity_array = []
        if self.rows > 0:
            for page in self.get_page_number(self.rows):
                url = self.base_url + '&'.join(
                    [
                        'wojewodztwo=' + str(self.parms['wojewodztwo']), 
                        'data-od=' + str(self.parms['data_od']), 
                        'data-do=' + str(self.parms['data_do']),
                        'pokaz-wszystkie-pola=true',
                        'limit=500',
                        f'page={str(page)}'
                    ]
                )
                respond = self.make_api_request(url)
                if respond:
                    for entity in respond['data']:
                        if entity:
                            self.parse_result(entity)
                            entity_array.append(self.attributes)
                time.sleep(random.randint(1, 4))
        return entity_array



client = cepik_operator()
start = datetime.date(2023, 11, 23)
end = datetime.date(2023, 11, 25)


client.set_wojewodztwo('02')
client.set_dates(start.strftime('%Y%m%d'), end.strftime('%Y%m%d'))
with open('test.json', "w", encoding='utf-8') as file:
    json.dump(client.get_entity_array(), file,  ensure_ascii=False, indent=4)
