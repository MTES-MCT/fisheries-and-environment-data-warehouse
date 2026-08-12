-- =====================================================================
-- Fixture MonitorEnv pour le rapport ULAM : mission de test 999100.
-- INSERT pur (pas de DELETE FROM), ID dédié distinct de tout ID réel.
-- Séparé en un fichier par table (au lieu d'un seul fichier à 5
-- INSERT) : Flyway exécute chaque fichier comme une transaction unique,
-- donc un échec sur une table annulait silencieusement les 4 autres.
-- Isoler chaque table permet de voir laquelle échoue réellement plutôt
-- que de tout perdre d'un coup.
-- Vérifié contre le schéma réel (github.com/MTES-MCT/monitorenv) :
-- missions.unit a été DROP (V0.046) ; mission_type (singulier) a été
-- remplacé par mission_types text[] (V0.072) ; mission_source est
-- NOT NULL sans défaut (V0.054), enum mission_source_type depuis
-- V0.064.1, valeur 'RAPPORT_NAV' ajoutée en V0.168.
-- =====================================================================

-- Mission de test : 10h en mer du 2025-06-02 08:00 au 18:00 UTC.
INSERT INTO public.missions (id, start_datetime_utc, end_datetime_utc, mission_types, mission_source, facade)
VALUES (999100, '2025-06-02 08:00:00', '2025-06-02 18:00:00', ARRAY['SEA'], 'RAPPORT_NAV', 'MED');
