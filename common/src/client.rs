/// Generic client to message a handler.
/// Only specify id on handlers that require deduplication (e.g., actors).
struct GenericClient {
    namespace: String,
    name: String,
    identifier: Option<String>,
}


impl GenericClient {
    
}

