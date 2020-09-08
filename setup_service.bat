
call nssm.exe install mis_freq_volt_service "%cd%\run_server.bat"
rem call nssm.exe edit mis_freq_volt_service
call nssm.exe set mis_freq_volt_service AppStdout "%cd%\logs\mis_freq_volt_service.log"
call nssm.exe set mis_freq_volt_service AppStderr "%cd%\logs\mis_freq_volt_service.log"
call sc start mis_freq_volt_service