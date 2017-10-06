package main

import (
	"flag"
	"os"
	"log"
	"errors"
	"net/http"
	"io"
	"encoding/json"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/kubernetes"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	errors2 "k8s.io/apimachinery/pkg/api/errors"
)

func main() {
	log.Print("Starting Kafka Connect Plugins controller")

	kubeconfig := ""
	namespace := "default"
	configMapName := "kafka-connect-plugins"
	pluginPath := "/opt/kafka/plugins-ext"

	flag.StringVar(&kubeconfig, "kubeconfig", kubeconfig, "kubeconfig file")
	flag.StringVar(&namespace, "namespace", namespace, "Kubernetes namespace")
	flag.StringVar(&configMapName, "config-map", configMapName, "Name of the config map with plugins / connectors")
	flag.StringVar(&pluginPath, "plugin-path", pluginPath, "Path where plugins should be downloaded")
	flag.Parse()

	if kubeconfig == "" {
		kubeconfig = os.Getenv("KUBECONFIG")
	}

	log.Printf("Parameter --kubeconfig set to: %v", kubeconfig)
	log.Printf("Parameter --namespace set to: %v", namespace)
	log.Printf("Parameter --config-map set to: %v", configMapName)
	log.Printf("Parameter --plugin-path set to: %v", pluginPath)

	var (
		config *rest.Config
		err    error
	)
	if kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		config, err = rest.InClusterConfig()
	}
	if err != nil {
		log.Fatalf("Error creating client: %v", err)
	}
	client := kubernetes.NewForConfigOrDie(config)

	configMap, err := client.CoreV1().ConfigMaps(namespace).Get(configMapName, metav1.GetOptions{})
	if err != nil {
		if errors2.IsNotFound(err) {
			log.Printf("The config map %s does not exist in namespace %s. No plugins will be downloaded.", configMapName, namespace)
			os.Exit(0)
		} else {
			log.Fatalf("Error getting the config map: %v", err)
		}
	}

	plugins := configMap.Data
	err = downloadPlugins(plugins, pluginPath)
	if err != nil {
		log.Fatalf("Failed to download plugins: %v", err)
	} else {
		log.Print("Plugins successfully downloaded")
		os.Exit(0)
	}
}

func downloadPlugins(pluginsToDownload map[string]string, pluginPath string) error {
	hasError := false

	for plugin, jarsJson := range pluginsToDownload {
		log.Printf("Downloading plugin %s", plugin)

		jars := make(map[string]string)

		err := json.Unmarshal([]byte(jarsJson), &jars)
		if err != nil {
			hasError = true
			log.Printf("Failed to decode JSON with jars %v", err)
			continue
		}

		filePath := pluginPath + "/" + plugin

		err = os.MkdirAll(filePath, 0755)
		if err != nil  {
			hasError = true
			log.Printf("Failed to created directory %s: %v", filePath, err)
			continue
		}

		for file, url := range jars {
			log.Printf("Downloading jar file %s from %s", file, url)

			outputFile, err := os.Create(filePath + "/" + file)
			if err != nil {
				hasError = true
				log.Printf("Failed to created new file plugin %s: %v", filePath + "/" + file, err)
				continue
			}

			defer outputFile.Close()

			downloadFile, err := http.Get(url)
			if err != nil {
				hasError = true
				log.Printf("Failed to start the download from %s: %v", url, err)
				continue
			}

			defer downloadFile.Body.Close()

			// Writer the body to file
			_, err = io.Copy(outputFile, downloadFile.Body)
			if err != nil {
				hasError = true
				log.Printf("Failed to download the plugin from %s to %s: %v", url, filePath + "/" + file, err)
				continue
			}
		}
	}

	if hasError {
		log.Print("Some plugins failed to download")
		return errors.New("Failed to download all plugins listed for deletion")
	} else {
		return nil
	}
}