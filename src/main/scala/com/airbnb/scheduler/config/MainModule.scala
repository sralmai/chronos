package com.airbnb.scheduler.config

import java.lang.Thread.UncaughtExceptionHandler
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.logging.{Level, Logger}

import com.airbnb.scheduler.mesos.{MesosTaskBuilder, MesosDriverFactory, MesosJobFramework}
import com.airbnb.scheduler.jobs.{JobMetrics, TaskManager, JobScheduler}
import com.airbnb.scheduler.graph.JobGraph
import com.airbnb.scheduler.state.PersistenceStore
import com.airbnb.notification.MailClient
import com.google.inject.{Inject, Provides, Singleton, AbstractModule}
import com.google.common.util.concurrent.{ListeningScheduledExecutorService, ThreadFactoryBuilder, MoreExecutors}
import com.twitter.common.zookeeper.Candidate
import org.apache.mesos.Protos.FrameworkInfo
import org.apache.mesos.Scheduler
import org.joda.time.Seconds
import mesosphere.mesos.util.FrameworkIdUtil
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.util.Timeout
import scala.concurrent.duration._
import org.slf4j.LoggerFactory

/**
 * Guice glue code of application logic components.
 * @author Florian Leibert (flo@leibert.de)
 */
class MainModule(val config: SchedulerConfiguration) extends AbstractModule {
  private[this] val log = LoggerFactory.getLogger(getClass)

  override def configure() {
    log.info("Wiring up the application")

    bind(classOf[SchedulerConfiguration]).toInstance(config)
    bind(classOf[Scheduler]).to(classOf[MesosJobFramework]).asEagerSingleton()
    bind(classOf[TaskManager]).asEagerSingleton()
    bind(classOf[MesosTaskBuilder]).asEagerSingleton()

    //TODO(FL): Only bind this if config.dependentJobs is turned on.
    bind(classOf[JobGraph]).asEagerSingleton()
  }

  @Inject
  @Singleton
  @Provides
  def provideFrameworkInfo(frameworkIdUtil: FrameworkIdUtil): FrameworkInfo = {
    val frameworkInfo = FrameworkInfo.newBuilder()
      .setName(config.mesosFrameworkName())
      .setCheckpoint(config.mesosCheckpoint())
      .setRole(config.mesosRole())
      .setFailoverTimeout(config.failoverTimeoutSeconds())
      .setUser(config.user())
    frameworkIdUtil.setIdIfExists(frameworkInfo)
    frameworkInfo.build()
  }


  @Singleton
  @Provides
  def provideMesosSchedulerDriverFactory(mesosScheduler: Scheduler, frameworkInfo: FrameworkInfo): MesosDriverFactory =
    new MesosDriverFactory(mesosScheduler, frameworkInfo, config)

  @Singleton
  @Provides
  def provideTaskScheduler(
                            taskManager: TaskManager,
                            dependencyScheduler: JobGraph,
                            persistenceStore: PersistenceStore,
                            mesosSchedulerDriver: MesosDriverFactory,
                            candidate: Candidate,
                            mailClient: Option[ActorRef],
                            metrics: JobMetrics): JobScheduler = {
    new JobScheduler(Seconds.seconds(config.scheduleHorizonSeconds()).toPeriod,
      taskManager, dependencyScheduler, persistenceStore,
      mesosSchedulerDriver, candidate, mailClient, config.failureRetryDelayMs(),
      config.disableAfterFailures(), metrics)
  }

  @Singleton
  @Provides
  def provideMailClient(): Option[ActorRef] = {
    for {
      server <- config.mailServer.get if !server.isEmpty && server.contains(":")
      from <- config.mailFrom.get if !from.isEmpty
    } yield {
      implicit val system = ActorSystem("chronos-actors")
      implicit val timeout = Timeout(36500 days)
      log.warn("Starting mail client.")
      system.actorOf(Props(classOf[MailClient], server, from,
        config.mailUser.get, config.mailPassword.get, config.mailSslOn()))
    }
  }

  @Singleton
  @Provides
  def provideListeningExecutorService(): ListeningScheduledExecutorService = {
    val uncaughtExceptionHandler = new UncaughtExceptionHandler {
      def uncaughtException(thread: Thread, t: Throwable) {
        log.warn("Error occurred in ListeningExecutorService, catching in thread", t)
      }
    }
    MoreExecutors.listeningDecorator(new ScheduledThreadPoolExecutor(5,
      new ThreadFactoryBuilder().setNameFormat("task_executor_thread-%d")
        .setUncaughtExceptionHandler(uncaughtExceptionHandler).build()))
  }
}
