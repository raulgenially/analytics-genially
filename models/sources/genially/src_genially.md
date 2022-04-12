{% docs subscription_codes %}

Type of subscription or plan.
One of the following values:

| subscription_code | definition |
|-------------------|------------|
| 1                 | Free       |
| 2                 | Pro        |
| 3                 | Master     |
| 4                 | Edu Pro    |
| 5                 | Edu Team   |
| 6                 | Team       |
| 7                 | Student    |

{% enddocs %}


{% docs collaboration_type %}

Type of collaboration.

One of the following values:

| collaboration_type |                   definition                    |
|--------------------|-------------------------------------------------|
| 1                  | Collaboration with a user in personal workspace |
| 2                  | Collaboration with a group in team workspace    |
| 4                  | Collaboration with a member in team workspace   |

{% enddocs %}


{% docs team_type %}

Type of team.

One of the following values:

| team_type | definition |
|-----------|------------|
| 1         | Free       |
| 2         | Master     |
| 3         | Edu        |

{% enddocs %}


{% docs team_space_collaborator_type %}

The type of id that we are dealing with in `collaborator_id`.

One of the following values:

| collaborator_type |  definition  |
|-------------------|--------------|
| 2                 | Team Group   |
| 4                 | Team Member  |

{% enddocs %}


{% docs team_member_role %}

Permission level of the member in the team.

One of the following values:

| collaborator_type |  definition  |
|-------------------|--------------|
| 1                 | Owner        |
| 2                 | Admin        |
| 3                 | Editor       |
| 4                 | Guest        |

{% enddocs %}

{% docs billing_description %}

Information about invoices and its refunds.

Relevant implementation events to take into account in this model:

| date |  event  |
|-------------------|--------------|
| 2019-06-12        | `transaction_id` is informed to identify invoices belonging to a `subscription_id` in the payment platform   |
| 2020-11-01        | The `description` field follows a new naming convention:<br>1 x Annual Student License Genial.ly (08 FEB 2022 - 08 FEB 2023)|
| 2021-12-21        | Invoices are informed with tax fields: `is_valid_euvat_number`, `tax_rate` and `tax_key` |

{% enddocs %}
