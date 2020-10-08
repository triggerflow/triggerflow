cp ../../../runtime/aws_lambda/triggerflow_handler.py .
zip -r downloader.zip bin/ downloader.py triggerflow_handler.py
zip -r executor.zip bin/ executor.py triggerflow_handler.py
zip -r orchestrator.zip bin/ orchestrator.py triggerflow_handler.py
rm triggerflow_handler.py
