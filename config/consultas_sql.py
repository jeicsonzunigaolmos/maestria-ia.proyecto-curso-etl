"""
    funciones que contienen todas las instrucciones sql que se van a utilizar 
    durante el proceso de extraccion, transformacion y carga de datos.
"""
def crear_tabla_apl1_contrato_digital():
    sql = """
        CREATE TABLE IF NOT EXISTS apl1_contrato_digital (
            id SERIAL PRIMARY KEY,
            nombre_entidad VARCHAR(4000),
            nit_entidad VARCHAR(4000),
            departamento VARCHAR(4000),
            ciudad VARCHAR(4000),
            localizaci_n VARCHAR(4000),
            orden VARCHAR(4000),
            sector VARCHAR(4000),
            rama VARCHAR(4000),
            entidad_centralizada VARCHAR(4000),
            proceso_de_compra VARCHAR(4000),
            id_contrato VARCHAR(4000),
            referencia_del_contrato VARCHAR(4000),
            estado_contrato VARCHAR(4000),
            codigo_de_categoria_principal VARCHAR(4000),
            descripcion_del_proceso VARCHAR(4000),
            tipo_de_contrato VARCHAR(4000),
            modalidad_de_contratacion VARCHAR(4000),
            justificacion_modalidad_de VARCHAR(4000),
            fecha_de_firma TIMESTAMP,
            fecha_de_inicio_del_contrato TIMESTAMP,
            fecha_de_fin_del_contrato TIMESTAMP,
            condiciones_de_entrega VARCHAR(4000),
            tipodocproveedor VARCHAR(4000),
            documento_proveedor VARCHAR(4000),
            proveedor_adjudicado VARCHAR(4000),
            es_grupo VARCHAR(4000),
            es_pyme VARCHAR(4000),
            habilita_pago_adelantado VARCHAR(4000),
            liquidaci_n VARCHAR(4000),
            obligaci_n_ambiental VARCHAR(4000),
            obligaciones_postconsumo VARCHAR(4000),
            reversion VARCHAR(4000),
            origen_de_los_recursos VARCHAR(4000),
            destino_gasto VARCHAR(4000),
            valor_del_contrato BIGINT,
            valor_de_pago_adelantado BIGINT,
            valor_facturado BIGINT,
            valor_pendiente_de_pago BIGINT,
            valor_pagado BIGINT,
            valor_amortizado BIGINT,
            valor_pendiente_de BIGINT,
            valor_pendiente_de_ejecucion BIGINT,
            estado_bpin VARCHAR(4000),
            c_digo_bpin VARCHAR(4000),
            anno_bpin VARCHAR(4000),
            saldo_cdp VARCHAR(4000),
            saldo_vigencia VARCHAR(4000),
            espostconflicto VARCHAR(4000),
            dias_adicionados VARCHAR(4000),
            puntos_del_acuerdo VARCHAR(4000),
            pilares_del_acuerdo VARCHAR(4000),
            urlproceso VARCHAR(4000),
            nombre_representante_legal VARCHAR(4000),
            nacionalidad_representante_legal VARCHAR(4000),
            domicilio_representante_legal VARCHAR(4000),
            tipo_de_identificaci_n_representante_legal VARCHAR(4000),
            identificaci_n_representante_legal VARCHAR(4000),
            g_nero_representante_legal VARCHAR(4000),
            presupuesto_general_de_la_nacion_pgn VARCHAR(4000),
            sistema_general_de_participaciones VARCHAR(4000),
            sistema_general_de_regal_as VARCHAR(4000),
            recursos_propios_alcald_as_gobernaciones_y_resguardos_ind_genas_ VARCHAR(4000),
            recursos_de_credito VARCHAR(4000),
            recursos_propios VARCHAR(4000),
            codigo_entidad VARCHAR(4000),
            codigo_proveedor VARCHAR(4000),
            objeto_del_contrato VARCHAR(4000),
            duraci_n_del_contrato VARCHAR(4000),
            nombre_del_banco VARCHAR(4000),
            tipo_de_cuenta VARCHAR(4000),
            n_mero_de_cuenta VARCHAR(4000),
            el_contrato_puede_ser_prorrogado VARCHAR(4000),
            nombre_ordenador_del_gasto VARCHAR(4000),
            tipo_de_documento_ordenador_del_gasto VARCHAR(4000),
            n_mero_de_documento_ordenador_del_gasto VARCHAR(4000),
            nombre_supervisor VARCHAR(4000),
            tipo_de_documento_supervisor VARCHAR(4000),
            n_mero_de_documento_supervisor VARCHAR(4000),
            nombre_ordenador_de_pago VARCHAR(4000),
            tipo_de_documento_ordenador_de_pago VARCHAR(4000),
            n_mero_de_documento_ordenador_de_pago VARCHAR(4000)
        );"""
    return sql
def limpiar_tabla_apl1_contrato_digital():
    sql = "delete from apl1_contrato_digital"
    return sql
def insert_apl1_contrato_digital():
    sql = """
    INSERT INTO apl1_contrato_digital (nombre_entidad
		,   nit_entidad
		,   departamento
		,   ciudad
		,   localizaci_n
		,   orden
		,   sector
		,   rama
		,   entidad_centralizada
		,   proceso_de_compra
		,   id_contrato
		,   referencia_del_contrato
		,   estado_contrato
		,   codigo_de_categoria_principal
		,   descripcion_del_proceso
		,   tipo_de_contrato
		,   modalidad_de_contratacion
		,   justificacion_modalidad_de
		,   fecha_de_firma
		,   fecha_de_inicio_del_contrato
		,   fecha_de_fin_del_contrato
		,   condiciones_de_entrega
		,   tipodocproveedor
		,   documento_proveedor
		,   proveedor_adjudicado
		,   es_grupo
		,   es_pyme
		,   habilita_pago_adelantado
		,   liquidaci_n
		,   obligaci_n_ambiental
		,   obligaciones_postconsumo
		,   reversion
		,   origen_de_los_recursos
		,   destino_gasto
		,   valor_del_contrato
		,   valor_de_pago_adelantado
		,   valor_facturado
		,   valor_pendiente_de_pago
		,   valor_pagado
		,   valor_amortizado
		,   valor_pendiente_de
		,   valor_pendiente_de_ejecucion
		,   estado_bpin
		,   c_digo_bpin
		,   anno_bpin
		,   saldo_cdp
		,   saldo_vigencia
		,   espostconflicto
		,   dias_adicionados
		,   puntos_del_acuerdo
		,   pilares_del_acuerdo
		,   urlproceso
		,   nombre_representante_legal
		,   nacionalidad_representante_legal
		,   domicilio_representante_legal
		,   tipo_de_identificaci_n_representante_legal
		,   identificaci_n_representante_legal
		,   g_nero_representante_legal
		,   presupuesto_general_de_la_nacion_pgn
		,   sistema_general_de_participaciones
		,   sistema_general_de_regal_as
		,   recursos_propios_alcald_as_gobernaciones_y_resguardos_ind_genas_
		,   recursos_de_credito
		,   recursos_propios
		,   codigo_entidad
		,   codigo_proveedor
		,   objeto_del_contrato
		,   duraci_n_del_contrato
		,   nombre_del_banco
		,   tipo_de_cuenta
		,   n_mero_de_cuenta
		,   el_contrato_puede_ser_prorrogado
		,   nombre_ordenador_del_gasto
		,   tipo_de_documento_ordenador_del_gasto
		,   n_mero_de_documento_ordenador_del_gasto
		,   nombre_supervisor
		,   tipo_de_documento_supervisor
		,   n_mero_de_documento_supervisor
		,   nombre_ordenador_de_pago
		,   tipo_de_documento_ordenador_de_pago
		,   n_mero_de_documento_ordenador_de_pago
	) VALUES (:nombre_entidad
		,   :nit_entidad
		,   :departamento
		,   :ciudad
		,   :localizaci_n
		,   :orden
		,   :sector
		,   :rama
		,   :entidad_centralizada
		,   :proceso_de_compra
		,   :id_contrato
		,   :referencia_del_contrato
		,   :estado_contrato
		,   :codigo_de_categoria_principal
		,   :descripcion_del_proceso
		,   :tipo_de_contrato
		,   :modalidad_de_contratacion
		,   :justificacion_modalidad_de
		,   :fecha_de_firma
		,   :fecha_de_inicio_del_contrato
		,   :fecha_de_fin_del_contrato
		,   :condiciones_de_entrega
		,   :tipodocproveedor
		,   :documento_proveedor
		,   :proveedor_adjudicado
		,   :es_grupo
		,   :es_pyme
		,   :habilita_pago_adelantado
		,   :liquidaci_n
		,   :obligaci_n_ambiental
		,   :obligaciones_postconsumo
		,   :reversion
		,   :origen_de_los_recursos
		,   :destino_gasto
		,   :valor_del_contrato
		,   :valor_de_pago_adelantado
		,   :valor_facturado
		,   :valor_pendiente_de_pago
		,   :valor_pagado
		,   :valor_amortizado
		,   :valor_pendiente_de
		,   :valor_pendiente_de_ejecucion
		,   :estado_bpin
		,   :c_digo_bpin
		,   :anno_bpin
		,   :saldo_cdp
		,   :saldo_vigencia
		,   :espostconflicto
		,   :dias_adicionados
		,   :puntos_del_acuerdo
		,   :pilares_del_acuerdo
		,   :urlproceso
		,   :nombre_representante_legal
		,   :nacionalidad_representante_legal
		,   :domicilio_representante_legal
		,   :tipo_de_identificaci_n_representante_legal
		,   :identificaci_n_representante_legal
		,   :g_nero_representante_legal
		,   :presupuesto_general_de_la_nacion_pgn
		,   :sistema_general_de_participaciones
		,   :sistema_general_de_regal_as
		,   :recursos_propios_alcald_as_gobernaciones_y_resguardos_ind_genas_
		,   :recursos_de_credito
		,   :recursos_propios
		,   :codigo_entidad
		,   :codigo_proveedor
		,   :objeto_del_contrato
		,   :duraci_n_del_contrato
		,   :nombre_del_banco
		,   :tipo_de_cuenta
		,   :n_mero_de_cuenta
		,   :el_contrato_puede_ser_prorrogado
		,   :nombre_ordenador_del_gasto
		,   :tipo_de_documento_ordenador_del_gasto
		,   :n_mero_de_documento_ordenador_del_gasto
		,   :nombre_supervisor
		,   :tipo_de_documento_supervisor
		,   :n_mero_de_documento_supervisor
		,   :nombre_ordenador_de_pago
		,   :tipo_de_documento_ordenador_de_pago
		,   :n_mero_de_documento_ordenador_de_pago
	)"""
    return sql
def consultar_datos_apl1_contrato_digital():
    sql = """
		select departamento
        ,	ciudad
        ,	orden
        ,	sector
        ,	rama
        ,	estado_contrato
        ,	tipo_de_contrato
        ,	TO_CHAR(fecha_de_firma, 'DD/MM/YYYY') as fecha_de_firma
        ,	TO_CHAR(fecha_de_inicio_del_contrato, 'DD/MM/YYYY') as fecha_de_inicio_del_contrato
        ,	TO_CHAR(fecha_de_fin_del_contrato, 'DD/MM/YYYY') as fecha_de_fin_del_contrato
        ,	destino_gasto
        ,	valor_del_contrato
        ,	valor_pendiente_de_pago
        ,	tipodocproveedor
        ,	documento_proveedor
        ,	proveedor_adjudicado
        ,	descripcion_del_proceso
 		from apl1_contrato_digital
	"""
    return sql
def crear_tabla_apl1_consolidado():
    sql = """
        CREATE TABLE IF NOT EXISTS apl1_consolidado (
            id SERIAL PRIMARY KEY,   
            departamento VARCHAR(4000),
            ciudad VARCHAR(4000),
            orden VARCHAR(4000),
            sector VARCHAR(4000),
            rama VARCHAR(4000),
            estado_contrato VARCHAR(4000),
            tipo_de_contrato VARCHAR(4000),
            fecha_de_firma TIMESTAMP,
            fecha_de_inicio_del_contrato TIMESTAMP,
            fecha_de_fin_del_contrato TIMESTAMP,
            destino_gasto VARCHAR(4000),
            valor_del_contrato BIGINT,
            valor_pendiente_de_pago BIGINT,
            tipodocproveedor VARCHAR(4000),
            documento_proveedor VARCHAR(4000),
            proveedor_adjudicado VARCHAR(4000),
            descripcion_del_proceso VARCHAR(4000),
            fecha_de_firma_dia BIGINT,
            fecha_de_firma_mes BIGINT,
            fecha_de_firma_anio BIGINT,
            fecha_de_inicio_del_contrato_dia BIGINT,
            fecha_de_inicio_del_contrato_mes BIGINT,
            fecha_de_inicio_del_contrato_anio BIGINT,
            fecha_de_fin_del_contrato_dia BIGINT,
            fecha_de_fin_del_contrato_mes BIGINT,
            fecha_de_fin_del_contrato_anio BIGINT,
            clasificacion_valor_del_contrato VARCHAR(4000),
            valor_pendiente_por_ejecutar BIGINT,
            porcentaje_valor_contrato_ejecutado BIGINT,
            contratos_inferiores_2024_pendientes_pago VARCHAR(4000)
        );"""
    return sql
def limpiar_tabla_apl1_consolidado():
    sql = "delete from apl1_consolidado"
    return sql
def insert_apl1_consolidado():
    sql = """
    	INSERT INTO apl1_consolidado (departamento,
            ciudad,
            orden,
            sector,
            rama,
            estado_contrato,
            tipo_de_contrato,
            fecha_de_firma,
            fecha_de_inicio_del_contrato,
            fecha_de_fin_del_contrato,
            destino_gasto,
            valor_del_contrato,
            valor_pendiente_de_pago,
            tipodocproveedor,
            documento_proveedor,
            proveedor_adjudicado,
            descripcion_del_proceso,
            fecha_de_firma_dia,
            fecha_de_firma_mes,
            fecha_de_firma_anio,
            fecha_de_inicio_del_contrato_dia,
            fecha_de_inicio_del_contrato_mes,
            fecha_de_inicio_del_contrato_anio,
            fecha_de_fin_del_contrato_dia,
            fecha_de_fin_del_contrato_mes,
            fecha_de_fin_del_contrato_anio,
            clasificacion_valor_del_contrato,
            valor_pendiente_por_ejecutar,
            porcentaje_valor_contrato_ejecutado,
            contratos_inferiores_2024_pendientes_pago
		) VALUES (:departamento,
            :ciudad,
            :orden,
            :sector,
            :rama,
            :estado_contrato,
            :tipo_de_contrato,
            :fecha_de_firma,
            :fecha_de_inicio_del_contrato,
            :fecha_de_fin_del_contrato,
            :destino_gasto,
            :valor_del_contrato,
            :valor_pendiente_de_pago,
            :tipodocproveedor,
            :documento_proveedor,
            :proveedor_adjudicado,
            :descripcion_del_proceso,
            :fecha_de_firma_dia,
            :fecha_de_firma_mes,
            :fecha_de_firma_anio,
            :fecha_de_inicio_del_contrato_dia,
            :fecha_de_inicio_del_contrato_mes,
            :fecha_de_inicio_del_contrato_anio,
            :fecha_de_fin_del_contrato_dia,
            :fecha_de_fin_del_contrato_mes,
            :fecha_de_fin_del_contrato_anio,
            :clasificacion_valor_del_contrato,
            :valor_pendiente_por_ejecutar,
            :porcentaje_valor_contrato_ejecutado,
            :contratos_inferiores_2024_pendientes_pago
		)"""
    return sql