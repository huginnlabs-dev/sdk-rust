//! Client-side PII classification: field names → privacy category labels.

const CATEGORIES: &[(&str, &[&str])] = &[
    ("password", &["password", "passwd", "pwd"]),
    ("secret", &["token", "secret", "apikey", "api_key", "credential", "session", "jwt", "auth"]),
    ("payment", &["card", "pan", "cvv", "cvc", "iban", "expiry"]),
    ("email", &["email", "e_mail", "mail"]),
    ("phone", &["phone", "mobile", "tel", "msisdn"]),
    ("government_id", &["ssn", "passport", "tax_id", "national_id"]),
    ("birth", &["birth", "dob", "age"]),
    ("name", &["first_name", "last_name", "full_name", "surname", "customer_name", "display_name"]),
    ("address", &["street", "zip", "postal", "street_address", "postal_address",
        "home_address", "billing_address", "shipping_address", "mailing_address"]),
    ("geo", &["city", "country", "region", "location", "lat", "lon", "lng"]),
    ("ip", &["ip", "ip_address", "client_ip", "remote_addr"]),
    ("device", &["device", "user_agent", "imei", "fingerprint"]),
];

/// Maps field names to deduplicated, comma-joined privacy categories.
/// Multiword keywords match by substring, single tokens by exact word.
pub fn classify(fields: &[String]) -> String {
    let mut seen: Vec<&str> = Vec::new();
    for field in fields {
        let norm: String = field
            .to_lowercase()
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        let tokens: Vec<&str> = norm.split('_').collect();
        for (cat, keywords) in CATEGORIES {
            if seen.contains(cat) {
                continue;
            }
            for kw in *keywords {
                let hit = if kw.contains('_') {
                    norm.contains(kw)
                } else {
                    tokens.contains(&kw)
                };
                if hit {
                    seen.push(cat);
                    break;
                }
            }
        }
    }
    seen.sort();
    seen.join(",")
}
