# backend repo search command for apis to support in humanfirst.apis.py
find . -name *.proto -exec grep -H -P "^\s*(get|put|post|delete).*v1alpha1" {} \; 