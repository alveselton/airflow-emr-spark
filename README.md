<h3> Airflow EMR Spark S3</h3>
Este projeto consiste em utilizar as tecnologias AIRFLOW, EMR (AWS), Spark e S3.
O objetivo e usar o fluxo do Airflow para criar um cluster EMR na AWS instalar automaticamente o Spark que após o processamento X o próprio Airflow irá finalizar o cluster e salvar o arquivo no S3

<h3> Requisitos: </h3> 
<ul>
<li>AIRFLOW instalado em uma distribuição LINUX</li>
<li>AWS CLI</li>
<li>VS Code</li>
</ul>

<h3>Roteiro</h3>
<ul>
<li>Executar o AIRFLOW em uma máquina Linux</li>
<li>Gerar o fluxo/script em uma DAG</li>
<li>Executar o fluxo, caso o mesmo não tenha sido programado</li>
<li>Aguardar a execução da criação do cluster na AWS</li>
<li>O arquivo e os logs serão salvos em um Bucket no S3 da AWS </li>
</ul>

<h3>Aplicação</h3>
Um cenário recomendado é quando tem a necessidade de processar um arquivo com milhares de informações, exemplo um log com milhões de registros. Utilizando o Spark e com script gerado em Python (utilizado neste caso), será distribuido entre os processadores disponives para obter o resultado desejado. 
