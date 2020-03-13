package com.joshlong.feed

import com.joshlong.jobs.watchdog.HeartbeatEvent
import com.joshlong.jobs.watchdog.WatchdogProperties
import com.rometools.rome.feed.synd.SyndEntry
import org.apache.commons.logging.LogFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.cloud.Cloud
import org.springframework.cloud.CloudFactory
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.ApplicationEventPublisherAware
import org.springframework.context.support.beans
import org.springframework.core.io.UrlResource
import org.springframework.data.redis.connection.RedisConnectionFactory
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
import java.time.Instant
import java.time.ZoneId
import java.util.*

/**
 * Monitors a collection of RSS or ATOM feeds and synchronizes them into a Pinboard account.
 *
 * This application dynamically stands up instances of the Spring Integration inbound ATOM/RSS feed adapter.
 * One interesting possibility is that we could refactor so that its ignorant of which feeds its processing, drawing
 * the actual feed information from configuration, incoming messages, or a database.
 *
 * This application is designed to be run as a Pivotal Cloud Foundry scheduled job. It'll startup, do some work and - if a
 * (configurable) period elapses with no new incoming elements - it'll shut itself down. The PCF scheduler
 * will wake it up at whatever specified interval we want. Outside of a PCF environment this is reproduced
 * using something like CRON.
 *
 * @author <a href="mailto:josh@joshlong.com">Josh Long</a>
 */
@SpringBootApplication
@EnableConfigurationProperties(IngestProperties::class)
class FeedApplication

class RedisMetadataStore(private val stringRedisTemplate: StringRedisTemplate) : MetadataStore {

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
class IngestProperties(val pollRateInSeconds: Long = 1)

@Component
class FeedIngestRunner(private val ifc: IntegrationFlowContext,
                       private val pc: PinboardClient,
                       private val ingestProperties: IngestProperties) :
		ApplicationRunner,
		ApplicationEventPublisherAware {

	private val log = LogFactory.getLog(javaClass)
	private var publisher: ApplicationEventPublisher? = null

	override fun setApplicationEventPublisher(p0: ApplicationEventPublisher) {
		this.publisher = p0
	}

	override fun run(args: ApplicationArguments) {
		val twisTag = "twis"
		mapOf(
				"https://blogs.vmware.com/all-vmware-blogs/wprss" to listOf("vmware", twisTag),
				"https://spring.io/blog.atom" to listOf("spring", twisTag),
				"https://cloudfoundry.org/feed/" to listOf("cloudfoundry", twisTag)
		)
				.forEach { (url, tags) ->
					val urlAsKey = url.filter { it.isLetterOrDigit() }
					val standardIntegrationFlow = IntegrationFlows
							.from(Feed.inboundAdapter(UrlResource(url), urlAsKey)) { pollerConfig ->
								pollerConfig.poller { it.fixedRate(ingestProperties.pollRateInSeconds * 1000) }
							}
							.handle(GenericHandler<SyndEntry> { syndEntry, _ ->
								processSyndEntry(syndEntry, tags)
								null
							})
							.get()
					ifc.registration(standardIntegrationFlow).id("flowForFeed${urlAsKey}").register()
				}
	}

	fun processSyndEntry(syndEntry: SyndEntry, incomingTags: List<String>) {
		val tags = incomingTags.map { it.toLowerCase() }
		val link = syndEntry.link
		val authors = mutableSetOf<String>()
				.apply {
					if (syndEntry.author != null && syndEntry.author.isNotBlank()) {
						add(syndEntry.author)
					}
					if (syndEntry.authors != null && syndEntry.authors.isNotEmpty()) {
						addAll(syndEntry.authors.map { it.name ?: "" })
					}
					filter { it.trim() != "" }
				}
		val title = syndEntry.title
		val date = Date(
				(syndEntry.updatedDate
						?: syndEntry.publishedDate
						?: Date()).time
		)
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
			this.publisher!!.publishEvent(HeartbeatEvent())
		} catch (ex: Exception) {
			log.error("couldn't process $link.", ex)
			ReflectionUtils.rethrowException(ex)
		}
	}
}

fun main() {
	runApplication<FeedApplication> {
		addInitializers(beans {
			beans {
				profile("cloud") {
					bean {
						CloudFactory().cloud
					}
					bean {
						ref<Cloud>().getSingletonServiceConnector(RedisConnectionFactory::class.java, null)
					}
				}
				bean {
					FeedIngestRunner(ref(), ref(), ref())
				}
				bean(IntegrationContextUtils.METADATA_STORE_BEAN_NAME) {
					RedisMetadataStore(ref())
				}
			}
		})
	}
}
