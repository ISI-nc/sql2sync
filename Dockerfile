# ------------------------------------------------------------------------
from mcluseau/golang-builder:1.12.5 as build

# ------------------------------------------------------------------------
from alpine:3.9
entrypoint ["/bin/sql2sync"]
copy --from=build /go/bin/ /bin/
