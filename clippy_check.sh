
# cargo clippy --workspace --locked --exclude hql-tests --exclude metrics -- -D warnings -A clippy::too_many_arguments -A clippy::let-and-return -A clippy::module-inception -A clippy::new-ret-no-self -A clippy::wrong-self-convention -A clippy::large-enum-variant -A clippy::inherent-to-string -A clippy::inherent_to_string_shadow_display -D clippy::unwrap_used

if [ "$1" = "dashboard" ]; then
    cargo clippy -p helix-container --features dev --features default  \
    -- -D warnings \
     -A clippy::too_many_arguments \
     -A clippy::let-and-return \
     -A clippy::module-inception \
     -A clippy::new-ret-no-self \
     -A clippy::wrong-self-convention \
     -A clippy::large-enum-variant \
     -A clippy::inherent-to-string \
     -A clippy::inherent_to_string_shadow_display
fi

cargo clippy --workspace --locked --exclude hql-tests --features default \
    -- -D warnings \
     -A clippy::too_many_arguments \
     -A clippy::let-and-return \
     -A clippy::module-inception \
     -A clippy::new-ret-no-self \
     -A clippy::wrong-self-convention \
     -A clippy::large-enum-variant \
     -A clippy::inherent-to-string \
     -A clippy::inherent_to_string_shadow_display
