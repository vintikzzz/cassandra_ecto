defmodule CassandraEctoHelperSpec do
  use ESpec, async: true

  alias Cassandra.Ecto.Helper
  describe "Cassandra.Ecto.Helper" do
    describe "quote_name/1" do
      it "wraps field name with quotes" do
        expect(Helper.quote_name("test")) |> to(eq "\"test\"")
      end
      it "fails with bad field name" do
        expect(fn -> Helper.quote_name("wro\"ng") end)
        |> to(raise_exception())
      end
    end
    describe "quote_table/1" do
      it "wraps table name with quotes" do
        expect(Helper.quote_table("test")) |> to(eq "\"test\"")
      end
      it "fails with bad table name" do
        expect(fn -> Helper.quote_table("wro\"ng") end)
        |> to(raise_exception())
      end
    end
    describe "db_value" do
      it "saves integers as is" do
        expect(Helper.db_value(1, :integer)) |> to(eq "1")
      end
      it "escapes strings with dollars" do
        expect(Helper.db_value("test", :string)) |> to(eq "$$test$$")
      end
      it "wraps :set with curly brackets" do
        expect(Helper.db_value([1, 2, 3], {:set, :int})) |> to(eq "{1, 2, 3}")
      end
      it "wraps :list with square brackets" do
        expect(Helper.db_value([1, 2, 3], {:list, :int})) |> to(eq "[1, 2, 3]")
      end
      it "saves map" do
        expect(Helper.db_value(%{"abra" => 1, "cadabra" => 2}, {:map, :string, :int}))
        |> to(eq "{$$abra$$: 1, $$cadabra$$: 2}")
      end
      it "saves map with skipped key type" do
        expect(Helper.db_value(%{"abra" => 1, "cadabra" => 2}, {:map, :int}))
        |> to(eq "{$$abra$$: 1, $$cadabra$$: 2}")
      end
      it "saves tuple" do
        expect(Helper.db_value({1, "abra"}, {:tuple, {:int, :string}}))
        |> to(eq "(1, $$abra$$)")
      end
      it "deals with nesting" do
        expect(Helper.db_value([{1, 2}, {3, 4}], {:list, {:tuple, {:int, :int}}}))
        |> to(eq "[(1, 2), (3, 4)]")
      end
    end
  end
end
