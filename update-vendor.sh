#!/bin/bash

go mod tidy
go mod vendor
git am patch/*
