CREATE TABLE foo (bar INTEGER ARRAY);

INSERT INTO foo VALUES
    ('{1, 2, 3}'),
    ('{}'),
    ('{1, 2, null}');