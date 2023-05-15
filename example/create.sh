#!/bin/bash

for i in {200..30000} ; do
  curl '127.0.0.1:8080/set?key=age'${i}'&value='${i}
done


