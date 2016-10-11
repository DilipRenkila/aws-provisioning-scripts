#!/bin/bash
aws ssm delete-document --name "Cahootsy-RunAdwordsScorer"
aws ssm create-document --content file://run_command.json --name "Cahootsy-RunAdwordsScorer"