# PayGine
Toy payment engine

## Assumptions
### Hard (app fails):
- Input path is valid.
- CSV format is valid: `"deposit, 1, 1, 1.0" or "resolve, 1, 1, "`.
- TX ids are unique.
- dispute/resolve/chargeback reference only valid tx id.