package main_test

import (
	"bytes"
	"testing"

	main "github.com/greenplum-db/gpupgrade/ci/scripts/filter"
)

func TestFilter(t *testing.T) {
	t.Run("it writes stdin to stdout when nothing is filtered", func(t *testing.T) {
		var in, out bytes.Buffer

		expected := "hello\n"
		in.WriteString(expected)

		main.Filter(&in, &out)
		if out.String() != expected {
			t.Errorf("wrote %q want %q", out.String(), expected)
		}
	})

	t.Run("filters out legacy hashops settings", func(t *testing.T) {
		var in, out bytes.Buffer

		in.WriteString(`
GRANT ALL ON DATABASE template1 TO gpadmin;
GRANT CONNECT ON DATABASE template1 TO PUBLIC;
ALTER DATABASE template1 SET gp_use_legacy_hashops TO 'on';
SET allow_system_table_mods = true;
CREATE DATABASE test WITH TEMPLATE = template0 OWNER = gpadmin;
RESET allow_system_table_mods;
ALTER DATABASE test SET gp_use_legacy_hashops TO 'on';
`)

		main.Filter(&in, &out)

		expected := `
GRANT ALL ON DATABASE template1 TO gpadmin;
GRANT CONNECT ON DATABASE template1 TO PUBLIC;
SET allow_system_table_mods = true;
CREATE DATABASE test WITH TEMPLATE = template0 OWNER = gpadmin;
RESET allow_system_table_mods;
`
		if out.String() != expected {
			t.Errorf("wrote %q want %q", out.String(), expected)
			t.Logf("actual (expanded): %s", out.String())
			t.Logf("expected (expanded): %s", expected)
		}
	})

	t.Run("filters out empty and commented lines attached to filtered SQL", func(t *testing.T) {
		var in, out bytes.Buffer

		in.WriteString(`
GRANT ALL ON DATABASE template1 TO gpadmin;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner:
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


RESET allow_system_table_mods;
`)

		main.Filter(&in, &out)

		expected := `
GRANT ALL ON DATABASE template1 TO gpadmin;


RESET allow_system_table_mods;
`
		if out.String() != expected {
			t.Errorf("wrote %q want %q", out.String(), expected)
			t.Logf("actual (expanded): %s", out.String())
			t.Logf("expected (expanded): %s", expected)
		}
	})

	t.Run("keeps trailing comment blocks", func(t *testing.T) {
		var in, out bytes.Buffer

		in.WriteString(`

--
-- Greenplum Database database dump complete
--

--
-- PostgreSQL database cluster dump complete
--

`)

		main.Filter(&in, &out)

		expected := `

--
-- Greenplum Database database dump complete
--

--
-- PostgreSQL database cluster dump complete
--

`
		if out.String() != expected {
			t.Errorf("wrote %q want %q", out.String(), expected)
			t.Logf("actual (expanded): %s", out.String())
			t.Logf("expected (expanded): %s", expected)
		}
	})
}
