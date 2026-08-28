-- Minimal deterministic catalog rows required by DB-bound worker tests.
-- Keep bulk conflict keys sorted to preserve the repository's deadlock-avoidance rule.

INSERT INTO albums (id, title, spotify_id)
VALUES (
  '10000000-0000-4000-8000-000000000001',
  'CI Fixture Album',
  'ci_fixture_album_001'
)
ON CONFLICT (spotify_id) DO NOTHING;

INSERT INTO genres (id, slug, label, position)
VALUES
  ('20000000-0000-4000-8000-000000000001', 'hip-hop', 'Hip-Hop', 1),
  ('20000000-0000-4000-8000-000000000002', 'rock', 'Rock', 2)
ON CONFLICT (slug) DO NOTHING;
