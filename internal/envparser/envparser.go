package envparser

import (
    "encoding/json"
    "fmt"
    "os"
    "strings"
)

const tempMarker = "__ESCAPED_DOLLAR__"

// ProcessJSONWithEnvVars prend une string JSON et retourne une string JSON
// avec les variables d'environnement substituées
func ProcessJSONWithEnvVars(inputJSON string) (string, error) {
    // 0. Vérifier qu'il n'y a pas de conflit avec le marqueur temporaire
    if strings.Contains(inputJSON, tempMarker) {
        return "", fmt.Errorf("le JSON contient le marqueur réservé: %s", tempMarker)
    }

    // 1. Vérifier que l'input est un JSON valide
    var data interface{}
    if err := json.Unmarshal([]byte(inputJSON), &data); err != nil {
        return "", fmt.Errorf("JSON invalide en entrée: %w", err)
    }

    // 2. Substituer les variables d'environnement
    processed := processValue(data)

    // 3. Convertir le résultat en JSON
    result, err := json.Marshal(processed)
    if err != nil {
        return "", fmt.Errorf("erreur lors de la conversion en JSON: %w", err)
    }

    return string(result), nil
}

// processValue traite récursivement les valeurs
func processValue(v interface{}) interface{} {
    switch v := v.(type) {
    case string:
        expanded, err := expandEnvWithEscape(v)
        if err != nil {
            // Dans un contexte réel, vous pourriez vouloir gérer cette erreur différemment
            return v
        }
        return expanded
    case map[string]interface{}:
        newMap := make(map[string]interface{})
        for key, value := range v {
            newMap[key] = processValue(value)
        }
        return newMap
    case []interface{}:
        newArray := make([]interface{}, len(v))
        for i, value := range v {
            newArray[i] = processValue(value)
        }
        return newArray
    default:
        return v
    }
}

// expandEnvWithEscape gère l'expansion des variables d'environnement avec échappement
func expandEnvWithEscape(s string) (string, error) {
    // Vérification supplémentaire pour le marqueur
    if strings.Contains(s, tempMarker) {
        return "", fmt.Errorf("la chaîne contient le marqueur réservé: %s", tempMarker)
    }

    // Remplacer temporairement les \$ par le marqueur
    temp := strings.ReplaceAll(s, `\$`, tempMarker)

    // Expansion des variables d'environnement
    expanded := os.ExpandEnv(temp)

    // Restaurer les $ échappés
    return strings.ReplaceAll(expanded, tempMarker, `$`), nil
}
