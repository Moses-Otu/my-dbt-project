
    
    

with child as (
    select host_sk as from_field
    from AIRBNB.DEV.dim_owners_cleansed
    where host_sk is not null
),

parent as (
    select host_sk as to_field
    from AIRBNB.DEV.dim_listings_w_owners
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


