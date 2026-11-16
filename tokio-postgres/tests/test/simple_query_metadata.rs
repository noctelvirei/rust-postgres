use super::connect;
use tokio_postgres::SimpleQueryMessage;

#[tokio::test]
async fn simple_query_columns_identify_table_attributes() {
    let client = connect("user=postgres").await;
    client
        .batch_execute(
            "CREATE TEMPORARY TABLE simple_metadata (id INT, name TEXT);
             INSERT INTO simple_metadata VALUES (1, 'name')",
        )
        .await
        .unwrap();
    let table_oid: u32 = client
        .query_one("SELECT 'simple_metadata'::regclass::oid", &[])
        .await
        .unwrap()
        .get(0);
    let messages = client
        .simple_query("SELECT name, id, 1 + 1 AS expression FROM simple_metadata")
        .await
        .unwrap();
    let SimpleQueryMessage::RowDescription(columns) = &messages[0] else {
        panic!("unexpected message");
    };
    assert_eq!(columns[0].table_oid(), table_oid);
    assert_eq!(columns[0].column_id(), 2);
    assert_eq!(columns[1].table_oid(), table_oid);
    assert_eq!(columns[1].column_id(), 1);
    assert_eq!(columns[2].table_oid(), 0);
    assert_eq!(columns[2].column_id(), 0);
    let SimpleQueryMessage::Row(row) = &messages[1] else {
        panic!("unexpected message");
    };
    assert_eq!(row.columns()[0].table_oid(), table_oid);
    assert_eq!(row.columns()[0].column_id(), 2);
    assert_eq!(row.columns()[2].table_oid(), 0);
    assert_eq!(row.columns()[2].column_id(), 0);
}

#[tokio::test]
async fn simple_query_command_tags_follow_row_counts() {
    let client = connect("user=postgres").await;
    let messages = client
        .simple_query(
            "CREATE TEMPORARY TABLE simple_tags (id INT);
             INSERT INTO simple_tags VALUES (1), (2);
             UPDATE simple_tags SET id = 3 WHERE false;
             SET application_name = 'simple_tags';
             SELECT id FROM simple_tags WHERE false;",
        )
        .await
        .unwrap();
    assert_eq!(messages.len(), 11);
    assert!(matches!(
        messages[0],
        SimpleQueryMessage::CommandComplete(0)
    ));
    assert!(matches!(&messages[1], SimpleQueryMessage::CommandTag(tag) if tag == "CREATE TABLE"));
    assert!(matches!(
        messages[2],
        SimpleQueryMessage::CommandComplete(2)
    ));
    assert!(matches!(&messages[3], SimpleQueryMessage::CommandTag(tag) if tag == "INSERT 0 2"));
    assert!(matches!(
        messages[4],
        SimpleQueryMessage::CommandComplete(0)
    ));
    assert!(matches!(&messages[5], SimpleQueryMessage::CommandTag(tag) if tag == "UPDATE 0"));
    assert!(matches!(
        messages[6],
        SimpleQueryMessage::CommandComplete(0)
    ));
    assert!(matches!(&messages[7], SimpleQueryMessage::CommandTag(tag) if tag == "SET"));
    assert!(matches!(messages[8], SimpleQueryMessage::RowDescription(_)));
    assert!(matches!(
        messages[9],
        SimpleQueryMessage::CommandComplete(0)
    ));
    assert!(matches!(&messages[10], SimpleQueryMessage::CommandTag(tag) if tag == "SELECT 0"));

    let messages = client.simple_query("").await.unwrap();
    assert_eq!(messages.len(), 1);
    assert!(matches!(
        messages[0],
        SimpleQueryMessage::CommandComplete(0)
    ));
}
