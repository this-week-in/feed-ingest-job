package com.example.feed

import com.rometools.rome.feed.synd.SyndEntry
import org.apache.commons.logging.LogFactory
import org.springframework.beans.factory.InitializingBean
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.support.GenericApplicationContext
import org.springframework.context.support.beans
import org.springframework.core.io.UrlResource
import org.springframework.core.task.TaskExecutor
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.integration.context.IntegrationContextUtils
import org.springframework.integration.dsl.IntegrationFlows
import org.springframework.integration.dsl.context.IntegrationFlowContext
import org.springframework.integration.feed.dsl.Feed
import org.springframework.integration.handler.GenericHandler
import org.springframework.integration.metadata.MetadataStore
import org.springframework.stereotype.Component
import org.springframework.util.ReflectionUtils
import pinboard.PinboardClient
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

/**
 * @author <a href="mailto:josh@joshlong.com">Josh Long</a>
 */
@SpringBootApplication
@EnableConfigurationProperties(IngestProperties::class)
class FeedApplication

class RedisMetadataStore(val stringRedisTemplate: StringRedisTemplate) : MetadataStore {

	override fun put(key: String, value: String) {
		stringRedisTemplate.opsForValue().set(key, value)
	}

	override fun remove(key: String): String? =
			if (stringRedisTemplate.hasKey(key)) {
				val existingValue = stringRedisTemplate.opsForValue().get(key)
				stringRedisTemplate.delete(key)
				existingValue
			} else null

	override fun get(key: String): String? =
			if (stringRedisTemplate.hasKey(key))
				stringRedisTemplate.opsForValue().get(key)
			else null
}

@ConfigurationProperties("ingest")
class IngestProperties(val inactivityThresholdInSeconds: Long = 60)

@Component
class InactivityMonitor(
		ingestProperties: IngestProperties,
		private val executor: TaskExecutor,
		private val applicationContext: GenericApplicationContext) : InitializingBean {

	private val window = Duration.ofSeconds(
			ingestProperties.inactivityThresholdInSeconds).toMillis()

	private val log = LogFactory.getLog(javaClass)

	private val lastTick = AtomicLong(System.currentTimeMillis())

	fun tick() {
		this.lastTick.set(System.currentTimeMillis())
	}

	override fun afterPropertiesSet() {
		this.executor.execute({
			this.log.info("About to begin ${javaClass.name} thread.")
			while (true) {
				Thread.sleep(1000 * 1)
				val now = System.currentTimeMillis()
				val then = this.lastTick.get()
				val diff = now - then
				if (diff > window) {
					this.log.info("There has been ${window}ms of inactivity. " +
							"Calling ${applicationContext.javaClass.name}#close()")
					this.applicationContext.close()
				}
			}
		})
	}
}

@Component
class SimpleRunner(val ifc: IntegrationFlowContext,
                   val pc: PinboardClient,
                   val inactivityMonitor: InactivityMonitor) : ApplicationRunner {

	private val log = LogFactory.getLog(javaClass)

	override fun run(args: ApplicationArguments) {

		val feeds = mapOf(
				"https://spring.io/blog.atom" to listOf("spring", "twis"),
				"https://cloudfoundry.org/feed/" to listOf("cloudfoundry", "twis"))

		feeds.keys.forEach { url ->
			val tags = feeds[url]
			val urlAsKey = url.filter { it.isLetterOrDigit() }
			val standardIntegrationFlow = IntegrationFlows
					.from(Feed.inboundAdapter(UrlResource(url), urlAsKey), { it.poller({ it.fixedRate(500) }) })
					.handle(GenericHandler<SyndEntry> { syndEntry, headers ->
						processSyndEntry(syndEntry, tags!!)
					})
					.get()
			val flowRegistration = ifc.registration(standardIntegrationFlow)
					.id("flowForFeed${urlAsKey}")
					.register()
		}
	}

	fun processSyndEntry(syndEntry: SyndEntry, incomingTags: List<String>) {

		val tags = incomingTags.map { it.toLowerCase() }
		val link = syndEntry.link
		val authors: Set<String> = hashSetOf<String>()
				.also { a ->
					if (syndEntry.author != null && syndEntry.author.isNotBlank()) {
						a.add(syndEntry.author)
					}
					if (syndEntry.authors != null && syndEntry.authors.isNotEmpty()) {
						a.addAll(syndEntry.authors.map { it.name ?: "" })
					}
					a.filter { it.trim() != "" }
				}
		val title = syndEntry.title
		val date = Date((syndEntry.updatedDate ?: syndEntry.publishedDate ?: Date()).time)
		try {
			log.info("Processing $link")
			if (pc.getPosts(url = link).posts.isEmpty()) {
				val post = pc.addPost(url = link, description = title,
						tags = tags.toTypedArray(), dt = date, extended = "by ${authors.joinToString(" and ")}",
						shared = false, toread = false, replace = false)
				if (post) {
					log.info("added $link ('$title') to Pinboard @ ${Instant.now().atZone(ZoneId.systemDefault())}")
				}
			}
			this.inactivityMonitor.tick()
		} catch (ex: Exception) {
			log.error("couldn't process $link.", ex)
			ReflectionUtils.rethrowException(ex)
		}
	}
}

fun main(args: Array<String>) {
	SpringApplicationBuilder()
			.initializers(
					beans {
						bean {
							Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors())
						}
						bean {
							SimpleRunner(ref(), ref(), ref())
						}
						bean(IntegrationContextUtils.METADATA_STORE_BEAN_NAME) {
							RedisMetadataStore(ref())
						}
					}
			)
			.sources(FeedApplication::class.java)
			.run(*args)
}