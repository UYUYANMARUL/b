use actix_cors::Cors;
use actix_web::middleware::Logger;
use actix_web::Responder;
use actix_web::{get, http::header, web, App, HttpServer};
use dotenv::dotenv;
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::prelude::FromRow;
use validator::{Validate, ValidationError};

#[derive(Debug, Deserialize, Serialize)]
enum TransactionType {
    Send,
    Receive,
}

fn default_page_size() -> i64 {
    25
}

fn default_page() -> i64 {
    0
}

#[derive(Debug, Deserialize, Serialize)]
// #[validate(schema(function = "validate_pagination", skip_on_field_errors = false))]
struct TransactionQuery {
    start_date: Option<chrono::DateTime<chrono::Utc>>,
    end_date: Option<chrono::DateTime<chrono::Utc>>,
    account_address: String,
    counterparty_address: Option<String>,
    contract_address: Option<Vec<String>>,
    transaction_type: Option<TransactionType>,
    order: Option<String>,
    #[serde(default = "default_page")]
    pub page: i64,
    #[serde(default = "default_page_size")]
    pub size: i64,
}

#[derive(Debug)]
pub enum Bind {
    INT(i64),
    STRING(String),
    TIMESTAMP(chrono::DateTime<chrono::Utc>),
}

#[derive(Debug, sqlx::FromRow, Serialize)]
struct Transfer {
    network: String,
    block_hash: String,
    block_number: i64,
    block_timestamp: chrono::DateTime<chrono::Utc>,
    transaction_hash: String,
    event_index: i64,
    contract_address: String,
    from_address: String,
    to_address: String,
    amount_raw: String,
}

#[derive(Debug, sqlx::FromRow, Serialize)]
struct Token {
    name: String,
    symbol: String,
    decimal: String,
    contract: String,
    image: String,
}

#[get("/tokens")]
async fn get_tokens(data: web::Data<AppState>) -> impl Responder {
    return match sqlx::query_as::<_, Token>("select * from stark_mainnet_coins")
        .fetch_all(&data.db)
        .await
    {
        Ok(tokens) => serde_json::to_string(&tokens),
        _ => Ok("".to_string()),
    };
}

// this handler gets called if the query deserializes into `Info` successfully
// otherwise a 400 Bad Request error response is returned
#[get("/portfolio")]
async fn get_portfolio(
    data: web::Data<AppState>,
    info: web::Query<TransactionQuery>,
) -> impl Responder {
    // Validate the order parameter to prevent SQL injection
    // let order = match order.to_uppercase().as_str() {
    //     "ASC" => "ASC",
    //     "DESC" => "DESC",
    //     _ => return Err(sqlx::Error::Protocol("Invalid order parameter".into())),
    // };

    // Start building the SQL query
    let mut sql = format!(
        r#"
        SELECT *
        FROM transfers
        JOIN stark_mainnet_coins ON transfers.contract_address = stark_mainnet_coins.contract
        WHERE 1 = 1
        "#
    );

    // Vector to hold query parameters
    let mut params: Vec<Bind> = Vec::new();

    match info.transaction_type {
        Some(TransactionType::Send) => {
            sql.push_str(" AND from_address = $");
            sql.push_str(&(params.len() + 1).to_string());
            params.push(Bind::STRING(info.account_address.clone()));

            if let Some(to) = &info.counterparty_address {
                sql.push_str(" AND to_address = $");
                sql.push_str(&(params.len() + 1).to_string());
                params.push(Bind::STRING(to.clone()));
            }
        }
        Some(TransactionType::Receive) => {
            sql.push_str(" AND to_address = $");
            sql.push_str(&(params.len() + 1).to_string());
            params.push(Bind::STRING(info.account_address.clone()));

            if let Some(from) = &info.counterparty_address {
                sql.push_str(" AND from_address = $");
                sql.push_str(&(params.len() + 1).to_string());
                params.push(Bind::STRING(from.clone()));
            }
        }
        None => {
            sql.push_str(" AND (from_address = $");
            sql.push_str(&(params.len() + 1).to_string());
            params.push(Bind::STRING(info.account_address.clone()));
            sql.push_str(" OR to_address = $");
            sql.push_str(&(params.len() + 1).to_string());
            sql.push_str(")");
            params.push(Bind::STRING(info.account_address.clone()));

            if let Some(counterparty) = &info.counterparty_address {
                sql.push_str(" AND (from_address = $");
                sql.push_str(&(params.len() + 1).to_string());
                params.push(Bind::STRING(counterparty.clone()));

                sql.push_str(" OR to_address = $");
                sql.push_str(&(params.len() + 1).to_string());
                sql.push_str(")");
                params.push(Bind::STRING(counterparty.clone()));
            }
        }
    }

    // Add start_date condition if provided
    if let Some(start) = &info.start_date {
        sql.push_str(" AND block_timestamp >= $");
        sql.push_str(&(params.len() + 1).to_string());
        params.push(Bind::TIMESTAMP(start.clone()));
    }

    // Add end_date condition if provided
    if let Some(end) = &info.end_date {
        sql.push_str(" AND block_timestamp <= $");
        sql.push_str(&(params.len() + 1).to_string());
        params.push(Bind::TIMESTAMP(end.clone()));
    }

    sql.push_str(" AND contract_address IN (SELECT contract FROM stark_mainnet_coins)");

    // Add ORDER BY clause
    sql.push_str(&format!(" ORDER BY block_number desc"));

    // Add LIMIT and OFFSET
    sql.push_str(" LIMIT $");
    sql.push_str(&(params.len() + 1).to_string());
    params.push(Bind::INT(info.size));

    sql.push_str(" OFFSET $");
    sql.push_str(&(params.len() + 1).to_string());
    params.push(Bind::INT(info.page * info.size));

    // Prepare the query
    let mut query = sqlx::query_as::<_, Transfer>(&sql);

    println!("{:?}", sql);

    // Bind each parameter according to its type.
    for param in params {
        println!("{:?}", param);
        match param {
            Bind::INT(i) => query = query.bind(i),
            Bind::STRING(s) => query = query.bind(s),
            Bind::TIMESTAMP(t) => query = query.bind(t),
        }
    }

    // Execute the query
    return match query.fetch_all(&data.db).await {
        Ok(transfers) => serde_json::to_string(&transfers),
        _ => Ok("".to_string()),
    };
}

pub struct AppState {
    db: PgPool,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "actix_web=info");
    }
    dotenv().ok();
    env_logger::init();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = match PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await
    {
        Ok(pool) => {
            println!("✅Connection to the database is successful!");
            pool
        }
        Err(err) => {
            println!("🔥 Failed to connect to the database: {:?}", err);
            std::process::exit(1);
        }
    };

    println!("🚀 Server started successfully :)");

    HttpServer::new(move || {
        let cors = Cors::default()
            .allowed_origin("http://localhost:3000")
            .allowed_methods(vec!["GET"])
            .allowed_headers(vec![
                header::CONTENT_TYPE,
                header::AUTHORIZATION,
                header::ACCEPT,
            ])
            .supports_credentials();

        App::new()
            .app_data(web::Data::new(AppState { db: pool.clone() }))
            .service(get_portfolio)
            .service(get_tokens)
            .wrap(cors)
            .wrap(Logger::default())
    })
    .bind(("127.0.0.1", 8000))?
    .run()
    .await
}
