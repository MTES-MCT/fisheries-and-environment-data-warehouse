DELETE FROM public.ports;

INSERT INTO public.ports (
    country_code_iso2, region,     locode,                    port_name, is_active, facade, latitude, longitude,         fao_areas) VALUES 
    (            'FR',   '56',     'FAKE', 'Fake Port initially active',      true, 'NAMO',     NULL,      NULL,              '{}'),
    (            'FR',   '56', 'PNO_PORT',         'Fake Port with PNO',     false,   NULL,     NULL,      NULL,              '{}'),
    (            'FR',   '56', 'LAN_PORT',         'Fake Port with LAN',     false, 'NAMO',     NULL,      NULL,              '{}'),
    (            'FR',  '999',    'FRCQF', 'Somewhere over the rainbow',     false, 'NAMO',     NULL,      NULL,              '{}'),
    (            'FR',  '999',    'FRBES',    'Somewhere over the hill',     false,   'SA',     45.5,      -1.2,        '{27.8.a}'),
    (            'FR',  '999',    'FRDPE',  'Somewhere over the clouds',     false,   NULL,     NULL,      NULL,              '{}'),
    (            'FR',  '999',    'FRDKK',   'Somewhere over the swell',     false, 'NAMO',     NULL,      NULL,              '{}'),
    (            'FR',  '999',    'FRLEH',   'Somewhere over the ocean',     false,   'SA',     46.2,     -1.18, '{27.8.a,27.8.b}'),
    (            'FR',  '999',    'FRZJZ',     'Somewhere over the top',     false,   'SA',     NULL,      NULL,              '{}'),
    (            'FR',  '999',    'GBPHD',           'Port with facade',     false, 'MEMN',     NULL,      NULL,              '{}');