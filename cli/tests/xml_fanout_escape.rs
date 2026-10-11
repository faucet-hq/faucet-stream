//! A parent record's value reaches an XML child's request body XML-escaped
//! (#862 M-071), not pasted in as markup.
#![cfg(all(
    feature = "source-rest",
    feature = "source-xml",
    feature = "sink-stdout"
))]

use serde_json::json;
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn a_fan_out_value_is_escaped_in_an_xml_body() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/customers"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({ "data": [{ "company": "A&B </Customer><Filter>" }] })),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/orders"))
        .and(body_string_contains(
            "<Customer>A&amp;B &lt;/Customer&gt;&lt;Filter&gt;</Customer>",
        ))
        .respond_with(
            ResponseTemplate::new(200).set_body_string("<Root><Item><id>1</id></Item></Root>"),
        )
        .expect(1)
        .mount(&server)
        .await;
    let uri = server.uri();
    let yaml = format!(
        r#"
version: 1
name: xml_fanout
pipeline:
  sources:
    customers:
      type: rest
      config: {{ base_url: "{uri}", path: /customers, records_path: "$.data[*]", pagination: {{ type: None }} }}
    orders:
      type: xml
      config:
        base_url: "{uri}"
        path: /orders
        method: POST
        auth: {{ type: none }}
        body: "<GetOrders><Customer>${{customers.company}}</Customer></GetOrders>"
        records_element_path: Root.Item
  sink: {{ type: stdout, config: {{}} }}
matrix:
  - id: customers
    source: {{ ref: customers }}
  - id: orders
    parent: customers
    source: {{ ref: orders }}
"#
    );
    faucet_cli::run_from_yaml_str(&yaml).await.unwrap();
    server.verify().await;
}
