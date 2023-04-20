package org.gbif.occurrence.ws.client;

import org.gbif.api.service.occurrence.DownloadLauncherService;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

@RequestMapping("launcher")
public interface DownloadLauncherWsClient extends DownloadLauncherService {

  @Override
  @DeleteMapping("/{jobId}")
  void cancelJob(@PathVariable("jobId") String jobId);

  @Override
  @DeleteMapping("/unlock")
  void unlockAll();

  @Override
  @DeleteMapping("/unlock/{jobId}")
  void unlock(@PathVariable("jobId") String jobId);
}
