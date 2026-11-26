#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C

# Approved pure libraries and their transitive normal/build dependencies.
# The isolated core uses no platform adapters or implicit clock.
external=(
    autocfg base64 block-buffer cfg-if chrono const-oid cpufeatures crypto-common
    digest displaydoc form_urlencoded hex hybrid-array icu_collections icu_locale_core
    icu_normalizer icu_normalizer_data icu_properties icu_properties_data icu_provider
    idna idna_adapter itoa libc litemap memchr num-traits percent-encoding potential_utf
    proc-macro2 quote serde serde_core serde_derive serde_json sha2 smallvec
    stable_deref_trait syn synstructure thiserror thiserror-impl tinystr typenum
    unicode-ident url utf8_iter writeable yoke yoke-derive zerofrom zerofrom-derive
    zerotrie zerovec zerovec-derive zmij
)

check_core() {
    local package=$1
    local tree unexpected features
    local -a allowed=("${external[@]}" "$@")
    local -a args=(-p "$package" --no-default-features --target all --locked --prefix none)

    tree=$(cargo tree "${args[@]}" --edges normal,build --format '{p}')
    unexpected=$(comm -23 \
        <(awk '{print $1}' <<< "$tree" | sort -u) \
        <(printf '%s\n' "${allowed[@]}" | sort -u))
    if [[ -n $unexpected ]]; then
        printf '%s: unexpected dependencies\n%s\n' "$package" "$unexpected" >&2
        return 1
    fi

    features=$(cargo tree "${args[@]}" --edges normal,build,features)
    if grep -E '^chrono feature "(clock|now)"' <<< "$features"; then
        printf '%s enables Chrono clock access\n' "$package" >&2
        return 1
    fi
}

# Resolve each package independently so feature unification cannot hide its actual closure.
check_core flow-dto
check_core flow-facts
check_core flow-policy flow-dto
check_core flow-domain flow-dto flow-facts
