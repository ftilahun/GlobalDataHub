SELECT
    COUNT(deduction_id)
FROM layer_deduction
    JOIN line
        ON layer_deduction.layer_id = line.layer_id
    JOIN layer
        ON layer_deduction.layer_id = layer.layer_id