DELETE FROM public.species;

INSERT INTO public.species (
        id, species_code,           species_name, scip_species_type) VALUES
    (    1,        'GHL',     'Pou Hasse Caille',        'DEMERSAL'),
    (    2,        'BFT',  '(Capitaine) Haddock',            'TUNA'),
    (    3,        'SWO', 'Friture sur la ligne',            'TUNA'),
    (    4,        'HKE',                'Merlu',        'DEMERSAL'),
    (    5,        'SOL',                 'Sole',        'DEMERSAL'),
    (    6,        'PIL',              'Sardine',         'PELAGIC'),
    (    7,        'COD',                'Morue',        'DEMERSAL');