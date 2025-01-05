# Absolutely not a Database (WorkTable)

`WorkTable` is in-memory (on-disk persistence is in progress currently) storage.

## Usage

`WorkTable` can be used just in user's code with `worktable!` macro. It will generate table struct and other related
structs that will be used for table logic.

```rust
worktable!(
    name: Test,
    columns: {
        id: u64 primary_key autoincrement,
        test: i64,
        another: u64 optional,
        exchange: String
    },
    indexes: {
        test_idx: test unique,
        exchnage_idx: exchange,
    }
    queries: {
        update: {
            AnotherByExchange(another) by exchange,
            AnotherByTest(another) by test,
            AnotherById(another) by id,
        },
        delete: {
            ByAnother() by another,
            ByExchange() by exchange,
            ByTest() by test,
        }
    }
);
```

- `name` field is used to define table's name, and is will be as prefix for generated objects. For example declaration
  above will generate struct `TestWorkTable`, so table struct will be `<name>WorkTable`.

    ```rust
    let table = TestWorkTable::default();
    let name = table.name();
    assert_eq!(name, "Test");
    ```
- `columns` field is used to define table's row schema. Default usage is `<column_name>: <type>`.
    - If user want to mark column as primary key `primary_key` flag is used. This flag can be used on multiple columns
      at a time. Primary key generation is also supported. For some basic types `autoincrement` is supported. Also
      `custom` generation is available. In this case user must provide his own implementation.
    ```rust
    #[derive(
      Archive,
      Debug,
      Default,
      Deserialize,
      Clone,
      Eq,
      From,
      PartialOrd,
      PartialEq,
      Ord,
      Serialize,
      SizeMeasure,
    )]
    #[rkyv(compare(PartialEq), derive(Debug))]
    struct CustomId(u64);

    #[derive(Debug, Default)]
    pub struct Generator(AtomicU64);

    impl PrimaryKeyGenerator<TestPrimaryKey> for Generator {
  
      fn next(&self) -> TestPrimaryKey {
        let res = self.0.fetch_add(1, Ordering::Relaxed);
        if res >= 10 {
            self.0.store(0, Ordering::Relaxed);
        }
        CustomId::from(res).into()
      }
    }

    impl TablePrimaryKey for TestPrimaryKey {
      type Generator = Generator;
    }

    worktable! (
      name: Test,
      columns: {
        id: CustomId primary_key custom,
        test: u64
      }
    );
    ```
    - If column field is `Option<T>`, `optional` flag can be used.
    - 