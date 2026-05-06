plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
    id("kotlin-parcelize")
    id("com.google.gms.google-services")
    id("com.google.firebase.crashlytics")
}

import com.android.build.api.variant.BuildConfigField
import com.android.build.api.variant.*
import java.util.Properties

fun javaStringLiteralForBuildConfig(value: String): String =
    '"' + value.replace("\\", "\\\\").replace("\"", "\\\"") + '"'

fun Project.loadEvolvesaDealerApiKey(): String {
    val fromEnv = System.getenv("EVOLVESA_DEALER_API_KEY")?.trim().orEmpty()
    if (fromEnv.isNotEmpty()) return fromEnv
    val fromProject = (findProperty("evolvesaDealerApiKey") as String?)?.trim().orEmpty()
    if (fromProject.isNotEmpty()) return fromProject
    val fromRoot = (rootProject.findProperty("evolvesaDealerApiKey") as String?)?.trim().orEmpty()
    if (fromRoot.isNotEmpty()) return fromRoot
    val f = rootProject.file("evolvesa-dealer.properties")
    if (!f.exists()) return ""
    val p = Properties()
    f.inputStream().use { p.load(it) }
    return p.getProperty("connector.api.key", "").trim()
}

android {
    namespace = "com.cubeone.scan"
    compileSdk = 34

    defaultConfig {
        applicationId = "com.cubeone.scan"
        minSdk = 24
        targetSdk = 34
        versionCode = 1
        versionName = "1.0"

        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"

        buildConfigField("Boolean", "LOCK_CONNECTOR_CONFIG", "false")
        buildConfigField("String", "LOCKED_CONNECTOR_BASE_URL", "\"\"")
        buildConfigField("String", "LOCKED_AUTH_BASE_URL", "\"\"")
        buildConfigField("String", "LOCKED_CONNECTOR_API_KEY", "\"\"")
        // POPIA / EvolveSA: CREDIT_CHECK must send payload.consentId after email approval (cubeone & evolvesa).
        buildConfigField("Boolean", "ENABLE_EMAIL_CONSENT_FLOW", "true")
    }

    flavorDimensions += "brand"
    productFlavors {
        create("cubeone") {
            dimension = "brand"
            resValue("string", "app_name", "CubeOneScan")
        }
        create("evolvesa") {
            dimension = "brand"
            applicationIdSuffix = ".evolvesa"
            versionNameSuffix = "-evolvesa"
            resValue("string", "app_name", "EvolveSA")
        }
    }

    val keystorePropsFile = rootProject.file("keystore.properties")
    val keystoreProps = Properties()
    val hasReleaseKeystore = keystorePropsFile.exists().also { exists ->
        if (exists) {
            keystoreProps.load(keystorePropsFile.inputStream())
        }
    }

    signingConfigs {
        create("release") {
            if (hasReleaseKeystore) {
                val storeFilePath = keystoreProps.getProperty("storeFile", "")
                if (storeFilePath.isNotBlank()) {
                    storeFile = rootProject.file(storeFilePath)
                }
                storePassword = keystoreProps.getProperty("storePassword", "")
                keyAlias = keystoreProps.getProperty("keyAlias", "")
                keyPassword = keystoreProps.getProperty("keyPassword", "")
            }
        }
    }

    buildTypes {
        debug {
            manifestPlaceholders["usesCleartextTraffic"] = "true"
        }
        release {
            isMinifyEnabled = false
            manifestPlaceholders["usesCleartextTraffic"] = "false"
            signingConfig = if (hasReleaseKeystore) {
                signingConfigs.getByName("release")
            } else {
                // Keeps local release builds possible until a keystore is provisioned.
                signingConfigs.getByName("debug")
            }
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro"
            )
        }
    }

    // Smaller APKs per CPU architecture for faster pilot installs.
    splits {
        abi {
            isEnable = true
            reset()
            include("arm64-v8a", "armeabi-v7a")
            isUniversalApk = true
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    kotlinOptions {
        jvmTarget = "17"
    }

    buildFeatures {
        viewBinding = true
        buildConfig = true
    }
}

// beforeVariants builders do not expose buildConfigFields on AGP 8.2 — use onVariants instead.
androidComponents {
    onVariants(selector().all()) { variant ->
        val n = variant.name.lowercase()
        // Dealer lock applies to evolvesaDebug and evolvesaRelease when a build-time key is supplied
        // (evolvesa-dealer.properties, EVOLVESA_DEALER_API_KEY, or -PevolvesaDealerApiKey).
        if (!n.contains("evolvesa")) return@onVariants
        val apiKey = project.loadEvolvesaDealerApiKey()
        if (apiKey.isEmpty()) return@onVariants
        val propsFile = rootProject.file("evolvesa-dealer.properties")
        val p = Properties()
        if (propsFile.exists()) propsFile.inputStream().use { p.load(it) }
        val base = p.getProperty("connector.base.url", "https://api.cubeone.co.za").trim()
        val auth = p.getProperty("auth.base.url", "https://auth.cubeone.co.za").trim()
        variant.buildConfigFields.put(
            "LOCK_CONNECTOR_CONFIG",
            BuildConfigField("Boolean", "true", null)
        )
        variant.buildConfigFields.put(
            "LOCKED_CONNECTOR_BASE_URL",
            BuildConfigField("String", javaStringLiteralForBuildConfig(base), null)
        )
        variant.buildConfigFields.put(
            "LOCKED_AUTH_BASE_URL",
            BuildConfigField("String", javaStringLiteralForBuildConfig(auth), null)
        )
        variant.buildConfigFields.put(
            "LOCKED_CONNECTOR_API_KEY",
            BuildConfigField("String", javaStringLiteralForBuildConfig(apiKey), null)
        )
    }
}

dependencies {
    // Core Android
    implementation("androidx.core:core-ktx:1.12.0")
    implementation("androidx.appcompat:appcompat:1.6.1")
    implementation("com.google.android.material:material:1.11.0")
    implementation("androidx.constraintlayout:constraintlayout:2.1.4")
    implementation("androidx.cardview:cardview:1.0.0")

    // Firebase Crashlytics (keep versions compatible with Kotlin 1.9.x toolchain)
    implementation(platform("com.google.firebase:firebase-bom:33.6.0"))
    implementation("com.google.firebase:firebase-crashlytics-ktx")

    // Security
    implementation("org.bouncycastle:bcprov-jdk15on:1.70")

    // CameraX
    val cameraxVersion = "1.3.1"
    implementation("androidx.camera:camera-core:$cameraxVersion")
    implementation("androidx.camera:camera-camera2:$cameraxVersion")
    implementation("androidx.camera:camera-lifecycle:$cameraxVersion")
    implementation("androidx.camera:camera-view:$cameraxVersion")

    // ML Kit Barcode Scanning
    implementation("com.google.mlkit:barcode-scanning:17.2.0")

    // Networking
    implementation("com.squareup.okhttp3:okhttp:4.12.0")

    // Coroutines
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.7.3")

    // Lifecycle
    implementation("androidx.lifecycle:lifecycle-runtime-ktx:2.6.2")
    implementation("androidx.lifecycle:lifecycle-viewmodel-ktx:2.6.2")
    implementation("androidx.swiperefreshlayout:swiperefreshlayout:1.1.0")
    implementation("androidx.recyclerview:recyclerview:1.3.2")
}

// Compatibility aliases for tools/configurations that still call non-flavor task names.
tasks.register("assembleDebugUnitTest") {
    group = "build"
    description = "Compatibility task: delegates to evolvesa debug unit test assembly."
    dependsOn("assembleEvolvesaDebugUnitTest")
}

tasks.register("testDebugUnitTest") {
    group = "verification"
    description = "Compatibility task: delegates to evolvesa debug unit tests."
    dependsOn("testEvolvesaDebugUnitTest")
}

tasks.register("assembleDebugAndroidTest") {
    group = "build"
    description = "Compatibility task: delegates to evolvesa debug androidTest assembly."
    dependsOn("assembleEvolvesaDebugAndroidTest")
}