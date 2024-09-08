use rand::RngCore;

#[cfg(not(feature = "turmoil"))]
pub fn rng() -> impl RngCore {
    rand::thread_rng()
}

#[cfg(feature = "turmoil")]
pub fn get_rng() -> impl RngCore {
    rand::thread_rng()
}
